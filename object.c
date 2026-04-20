// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions: object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Write an object to the store.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Build header: "<type> <size>\0"
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1; // +1 for \0

    // 2. Build full object = header + data
    size_t total_len = (size_t)header_len + len;
    uint8_t *full_obj = malloc(total_len);
    if (!full_obj) return -1;

    memcpy(full_obj, header, header_len);
    memcpy(full_obj + header_len, data, len);

    // 3. Compute SHA-256 of the full object
    ObjectID id;
    compute_hash(full_obj, total_len, &id);
    if (id_out) *id_out = id;

    // 4. Check for deduplication
    if (object_exists(&id)) {
        free(full_obj);
        return 0;
    }

    // 5. Build shard directory path and create it
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // Ignore error if already exists

    // 6. Build final path and temp path
    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/%.2s/%s.tmp", OBJECTS_DIR, hex, hex + 2);

    // 7. Write to temp file
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0444);
    if (fd < 0) {
        free(full_obj);
        return -1;
    }

    ssize_t written = write(fd, full_obj, total_len);
    free(full_obj);

    if (written != (ssize_t)total_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // 8. fsync the temp file
    fsync(fd);
    close(fd);

    // 9. Atomically rename to final path
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // 10. fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return 0;
}

// Read an object from the store.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) {
        fclose(f);
        return -1;
    }

    uint8_t *raw = malloc((size_t)file_size);
    if (!raw) {
        fclose(f);
        return -1;
    }

    size_t read_bytes = fread(raw, 1, (size_t)file_size, f);
    fclose(f);

    if (read_bytes != (size_t)file_size) {
        free(raw);
        return -1;
    }

    // Verify integrity FIRST before anything else
    ObjectID computed;
    compute_hash(raw, (size_t)file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1;  // Corruption detected
    }

    // Find null byte separating header from data
    uint8_t *null_byte = memchr(raw, '\0', (size_t)file_size);
    if (!null_byte) {
        free(raw);
        return -1;
    }

    size_t header_len = null_byte - raw;
    char header[64];
    if (header_len >= sizeof(header)) {
        free(raw);
        return -1;
    }
    memcpy(header, raw, header_len);
    header[header_len] = '\0';

    char type_str[16];
    size_t data_size;
    if (sscanf(header, "%15s %zu", type_str, &data_size) != 2) {
        free(raw);
        return -1;
    }

    if (type_out) {
        if      (strcmp(type_str, "blob")   == 0) *type_out = OBJ_BLOB;
        else if (strcmp(type_str, "tree")   == 0) *type_out = OBJ_TREE;
        else if (strcmp(type_str, "commit") == 0) *type_out = OBJ_COMMIT;
        else { free(raw); return -1; }
    }

    size_t actual_data_len = (size_t)file_size - (header_len + 1);
    if (actual_data_len != data_size) {
        free(raw);
        return -1;
    }

    uint8_t *out = malloc(actual_data_len + 1);
    if (!out) {
        free(raw);
        return -1;
    }
    memcpy(out, null_byte + 1, actual_data_len);
    out[actual_data_len] = '\0';

    free(raw);
    *data_out = out;
    *len_out = actual_data_len;
    return 0;
}
