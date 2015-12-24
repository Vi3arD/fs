#define FUSE_USE_VERSION 29

#include <errno.h>
#include <fuse.h>
#include <mcheck.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>


#define IMAGE_FILE_NAME "fs"
#define NAME_LENGTH 16
#define CLUSTER_SIZE 512
#define CLUSTER_COUNT 100
#define END_OF_FILE 0xFFFFFFFF
#define EMPTY 0x00000000

typedef unsigned int cluster_t;

struct file_entry_s {
	char name[NAME_LENGTH];
	mode_t mode;
	time_t created;
	cluster_t cluster;
	size_t size;
};

typedef struct file_entry_s file_entry;

int image;
off_t data_offset;
int entries_count;

int is_folder(file_entry file) {
	return file.mode & S_IFDIR;
}

cluster_t get_next_cluster(cluster_t cluster) {
	lseek(image, sizeof(cluster_t) * cluster, SEEK_SET);
	cluster_t next;
	read(image, &next, sizeof(cluster_t));
	return next;
}

int is_cluster_free(cluster_t cluster) {
	return get_next_cluster(cluster) == 0 ? 1 : 0;
}

cluster_t find_empty_cluster_after(cluster_t cluster) {
	while (cluster < CLUSTER_COUNT && !is_cluster_free(cluster)) {
		cluster++;
	}
	return cluster == CLUSTER_COUNT ? END_OF_FILE : cluster;
}

cluster_t find_empty_cluster() {
	return find_empty_cluster_after(0);
}

int set_next_cluster(cluster_t cluster, cluster_t next) {
	if (cluster == END_OF_FILE) {
		return 1;
	}
	lseek(image, sizeof(cluster_t) * cluster, SEEK_SET);
	cluster_t tmp = next;
	write(image, &tmp, sizeof(cluster_t));
	return 0;
}

int set_cluster_end_of_file(cluster_t cluster) {
	return set_next_cluster(cluster, END_OF_FILE);
}

int free_cluster(cluster_t cluster) {
	return set_next_cluster(cluster, 0);
}

cluster_t get_cluster_by_offset(cluster_t start, off_t offset) {
	int n = offset / CLUSTER_SIZE;
	int i = 0;
	cluster_t tmp = start;
	while (i != n) {
		tmp = get_next_cluster(tmp);
		++i;
	}
	return tmp;
}

cluster_t extend(cluster_t cluster) {
	cluster_t next = find_empty_cluster_after(cluster);
	lseek(image, sizeof(cluster_t) * cluster, SEEK_SET);
	write(image, &next, sizeof(next));
	set_cluster_end_of_file(next);
	printf("Extending cluster %d, result: %d\n", cluster, next);
	return next;
}

char* extract_folder(const char *path) {
	int length = strlen(path);
	int i = length - 1;
	while (i > 0 && path[i] != '/') {
		--i;
	}
	char *result = (char *)malloc(length+1);
	strcpy(result, path);
	result[i] = '\0';
	return result;
}

char* extract_filename(const char *path) {
	int length = strlen(path);
	int i = length - 1;
		while (i > 0 && path[i] != '/') {
		--i;
	}
	if (i < 0) {
		return NULL;
	}
	char *result = (char *)malloc(length - i);
	strcpy(result, path + i + 1);
	return result;
}

char* get_path(const char *folder_path, char *filename) {
	int n = strlen(folder_path);
	int m = strlen(filename);
	char *path = (char*)malloc(n + m + 3);
	strncpy(path, folder_path, n);
	path[n] = '/';
	memcpy(path + n + 1, filename, m);
	printf("Path: %s\n", path);
	return path;
}

int get_file_entry_from_cluster(const char *name, cluster_t cluster, file_entry *file) {
	int i;
	file_entry tmp;
	while (cluster != END_OF_FILE) {
		lseek(image, data_offset + cluster * CLUSTER_SIZE, SEEK_SET);
		for (i = 0; i<entries_count; i++) {
			read(image, &tmp, sizeof(file_entry));
			if (strcmp(name, (char*)tmp.name) == 0) {
				*file = tmp;
				return 0;
			}
		}
		cluster = get_next_cluster(cluster);
	}
	return 1;
}

int get_file_entry(const char *path, file_entry *file) {
	if (strcmp(path, "") == 0) {
		file_entry root;
		root.cluster = 0;
		root.mode = S_IFDIR | 0755;
		*file = root;
		return 0;
	}
	file_entry tmp;
	if (get_file_entry(extract_folder(path), &tmp) != 0) {
		return 1;
	}
	printf("File name: %s\n", extract_filename(path));
	if (get_file_entry_from_cluster(extract_filename(path), tmp.cluster, file) != 0) {
		return 1;
	}
	return 0;
}

off_t get_file_entry_offset(const char *path) {
	file_entry folder;
	get_file_entry(extract_folder(path), &folder);
	cluster_t cluster = folder.cluster;

	int i;
	file_entry buf;
	cluster_t tmp;
	do {
		off_t offset = data_offset + CLUSTER_SIZE * cluster;
		lseek(image, offset, SEEK_SET);
		for (i = 0; i<entries_count; i++) {
			read(image, &buf, sizeof(file_entry));
			if (strcmp((char*)buf.name, extract_filename(path)) == 0) {
				return offset;
			}
			offset += sizeof(file_entry);
		}
		tmp = cluster;
		cluster = get_next_cluster(cluster);
	} while (cluster != END_OF_FILE);
	return -1;
}


off_t find_empty_file_entry_offset(const char *path) {
	file_entry folder;
	get_file_entry(path, &folder);
	cluster_t cluster = folder.cluster;

	int i;
	file_entry buf;
	cluster_t tmp;
	do {
		off_t offset = data_offset + CLUSTER_SIZE * cluster;
		lseek(image, offset, SEEK_SET);
		for (i = 0; i<entries_count; i++) {
			read(image, &buf, sizeof(file_entry));
			if (strlen(buf.name) == 0) {
				return offset;
			}
			offset += sizeof(file_entry);
		}
		tmp = cluster;
		cluster = get_next_cluster(cluster);
	} while (cluster != END_OF_FILE);
	return sizeof(cluster_t) * extend(tmp);
}

int edit_file_entry_size(const char* path, size_t size) {
	file_entry file;
	if (get_file_entry(path, &file) != 0) {
		return 1;
	}
	off_t offset = get_file_entry_offset(path);
	file.size = size;
	lseek(image, offset, SEEK_SET);
	write(image, &file, sizeof(file));
	return 0;
}

int edit_file_entry_name(const char* path, char *name) {
	file_entry file;
	if (get_file_entry(path, &file) != 0) {
		return 1;
	}
	off_t offset = get_file_entry_offset(path);
	memcpy(file.name, name, NAME_LENGTH);
	lseek(image, offset, SEEK_SET);
	write(image, &file, sizeof(file));
	return 0;
}

int myfat_getattr(const char *path, struct stat *stat) {
	memset(stat, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stat->st_mode = S_IFDIR | 0755;
		stat->st_nlink = 2;
	} else {
		file_entry file;
		if (get_file_entry(path, &file) != 0) {
			return -ENOENT;
		}
		stat->st_mode = file.mode;
		stat->st_nlink = 1;
		stat->st_size = file.size;
		stat->st_mtime = file.created;
		stat->st_atime = file.created;
	}
	return 0;
}

int myfat_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *info) {
	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	file_entry folder;
	if (get_file_entry(path, &folder) != 0) {
		return -ENOENT;
	}
	cluster_t cluster = folder.cluster;

	int i;
	file_entry tmp;

	do {
		lseek(image, data_offset + cluster * CLUSTER_SIZE, SEEK_SET);
		for (i = 0; i<entries_count; i++) {
			read(image, &tmp, sizeof(file_entry));
			if (strlen(tmp.name) != 0) {
				filler(buf, (char*)tmp.name, NULL, 0);
			}
		}
		cluster = get_next_cluster(cluster);
	} while (cluster != END_OF_FILE);
	return 0;
}

int myfat_open (const char *path, struct fuse_file_info *info) {
	file_entry file;
	if (get_file_entry(path, &file) != 0) {
		return -ENOENT;
	}
	return 0;
}

int myfat_read(const char *path, char *buf, size_t buf_size, off_t offset, struct fuse_file_info *info) {
    printf("Start file read; path: %s; offset: %lld\n", path, offset);
    file_entry file;
    cluster_t cluster = 0;
    off_t cluster_offset = 0;
    size_t size = 0;
    if (get_file_entry(path, &file) != 0) {
        return -ENOENT;
    }
    if (offset < file.size) {
        if (offset + buf_size > file.size) {
            size = file.size - offset;
        } else {
            size = buf_size;
        }
        size_t sum = 0;
        cluster = get_cluster_by_offset(file.cluster, offset);
        char *tmp = (char*)malloc(CLUSTER_SIZE);
        size_t current = 0;
        printf("size %d\n", size);
        while (sum < size) {
            printf("cluster: %d; sum: %d\n", cluster, sum);
            cluster_offset = offset % CLUSTER_SIZE;
            if (sum + CLUSTER_SIZE < size) {
                current = CLUSTER_SIZE - cluster_offset;
            } else {
                current = size - sum - cluster_offset;
            }
            printf("cluster_offset: %lld; current: %d\n", cluster_offset, current);
            lseek(image, data_offset + cluster * CLUSTER_SIZE + cluster_offset, SEEK_SET);
            read(image, tmp, current);
            memcpy(buf + sum, tmp, current);
            offset += current;
            sum += current;
            cluster = get_next_cluster(cluster);
        }
        free(tmp);
    }
    printf("Start file read; path: %s\n", path);
	return size;
}

int create_file(const char *path, mode_t mode) {
	file_entry file;

	if (get_file_entry(extract_folder(path), &file) != 0) {
		return -ENOENT;
	}
	strncpy(file.name, extract_filename(path), NAME_LENGTH);
	file.mode = mode;
	time(&file.created);
	file.size = 0;
	file.cluster = find_empty_cluster();
	set_cluster_end_of_file(file.cluster);
	off_t offset = find_empty_file_entry_offset(extract_folder(path));
	lseek(image, offset, SEEK_SET);
	write(image, &file, sizeof(file_entry));
	return 0;
}

int myfat_mkdir(const char *path, mode_t mode) {
	return create_file(path, mode | S_IFDIR);
}

int myfat_create(const char *path, mode_t mode, struct fuse_file_info *info) {
	return create_file(path, mode | S_IFREG);
}

void free_all_file_clusters(file_entry file) {
	cluster_t file_cluster = file.cluster;
	cluster_t next;
	do {
		next = get_next_cluster(file_cluster);
		free_cluster(file_cluster);
		file_cluster = next;
		printf("Next cluster: %d\n", next);
	} while(next != END_OF_FILE);
}

int myfat_unlink(const char* path) {
	printf("Start removing file\n");
	file_entry file;
	if (get_file_entry(path, &file) != 0) {
		return -ENOENT;
		printf("File not found\n");
	}
	file_entry folder;
	get_file_entry(extract_folder(path), &folder);
	cluster_t folder_cluster = folder.cluster;
	printf("Folder cluster: %d\n", folder_cluster);
	printf("File cluster: %d\n", file.cluster);
	file_entry tmp;
	do {
		off_t offset = data_offset + folder_cluster * CLUSTER_SIZE;
		lseek(image, offset, SEEK_SET);
		int i;
		for (i = 0; i<entries_count; i++) {
			read(image, &tmp, sizeof(file_entry));
			if (strcmp((char*)tmp.name, extract_filename(path)) == 0) {
				printf("File is found\n");
				file_entry buf;
				memset(&buf, 0, sizeof(file_entry));
				lseek(image, offset, SEEK_SET);
				write(image, &buf, sizeof(file_entry));
				free_all_file_clusters(file);
				printf("Finish removing file\n");
				return 0;
			}
			offset += sizeof(file_entry);
		}
		folder_cluster = get_next_cluster(folder_cluster);
	} while (folder_cluster != END_OF_FILE);
	return 0;
}

int myfat_rmdir(const char *path) {
	printf("Start removing folder\n");
	file_entry folder;
	if (get_file_entry(path, &folder) != 0) {
		return -ENOENT;
		printf("Folder not found\n");
	}
	file_entry root;
	get_file_entry(extract_folder(path), &root);
	cluster_t root_cluster = root.cluster;
	printf("Root cluster: %d\n", root_cluster);
	printf("Folder cluster: %d\n", folder.cluster);
	file_entry tmp;
	file_entry buf;
	memset(&buf, 0, sizeof(file_entry));
	do {
		off_t offset = data_offset + root_cluster * CLUSTER_SIZE;
		lseek(image, offset, SEEK_SET);
		int i;
		for (i = 0; i<entries_count; i++) {
			read(image, &tmp, sizeof(file_entry));
			if (strcmp((char*)tmp.name, extract_filename(path)) == 0) {
				printf("Folder is here\n");
				lseek(image, offset, SEEK_SET);
				write(image, &buf, sizeof(file_entry));
				cluster_t folder_cluster = folder.cluster;
				do {
					lseek(image, data_offset + folder_cluster * CLUSTER_SIZE, SEEK_SET);
					int j;
					for (j = 0; j < entries_count; j++) {
						read(image, &tmp, sizeof(file_entry));
						if (strlen(tmp.name) != 0) {
							const char *file_path = get_path(path, (char*)tmp.name);
							if (is_folder(tmp)) {
								myfat_rmdir(file_path);
							} else {
								myfat_unlink(file_path);
							}
						}
					}
					folder_cluster = get_next_cluster(folder_cluster);
				} while(folder_cluster != END_OF_FILE);
				free_all_file_clusters(folder);
				printf("Finish removing folder\n");
				return 0;
			}
			offset += sizeof(file_entry);
		}
		root_cluster = get_next_cluster(root_cluster);
	} while (root_cluster != END_OF_FILE);
	return 0;
}

int myfat_rename (const char* old_path, const char* new_path) {
	file_entry new_folder;
	file_entry file;
	if (get_file_entry(extract_folder(new_path), &new_folder) != 0) {
		return -ENOENT;
	}
	if (get_file_entry(old_path, &file) != 0) {
		return -ENOENT;
	}
	edit_file_entry_name(old_path, extract_filename(new_path));

	if (strcmp(extract_folder(old_path), extract_folder(new_path)) != 0) {
		file_entry tmp;
		memset(&tmp, 0, sizeof(file_entry));
		off_t offset = get_file_entry_offset(old_path);
		lseek(image, offset, SEEK_SET);
		write(image, &tmp, sizeof(file_entry));
		offset = find_empty_file_entry_offset(new_path);
		lseek(image, offset, SEEK_SET);
		write(image, &file, SEEK_SET);
	}
	return 0;
}

int get_cluster_count(file_entry file) {
	int count = 1;
	cluster_t cluster = file.cluster;
	while (cluster != END_OF_FILE) {
		++count;
		cluster = get_next_cluster(cluster);
	}
	return count;
}

int myfat_truncate(const char *path, off_t size) {
	printf("Start file truncate; path: %s; size: %lld\n", path, size);
	file_entry file;
	if (get_file_entry(path, &file) != 0) {
		return -ENOENT;
	}
	int current_count = file.size / CLUSTER_SIZE + 1;
	int new_count = size / CLUSTER_SIZE  + 1;
	printf("file truncate current: %d; new: %d; file cluster: %d\n", current_count, new_count, file.cluster);
	edit_file_entry_size(path, size);
	cluster_t cluster = file.cluster;
	int i;
	if (new_count > current_count) {
		printf("truncate: extending\n");
		for (i = 0; i<current_count - 1; i++) {
			printf("truncate: cluster: %d\n", cluster);
			cluster = get_next_cluster(cluster);
		}
		for (i = current_count - 1; i<new_count; i++) {
			printf("truncate: cluster: %d\n", cluster);
			cluster = extend(cluster);
		}
	} else {
		printf("truncate: truncating\n");
		for (i = 0; i<new_count; i++) {
			printf("truncate: cluster: %d\n", cluster);
			cluster = get_next_cluster(cluster);
		}
		for (i = new_count; i<current_count; i++) {
			printf("truncate: cluster: %d\n", cluster);
			cluster_t next = get_next_cluster(cluster);
			free_cluster(cluster);
			cluster = next;
		}
	}
	printf("finish file truncate; path: %s\n", path);
	return 0;
}


int myfat_write(const char* path, const char *buf, size_t buf_size, off_t offset, struct fuse_file_info* fi) {
    printf("Start file write; path: %s; offset: %lld, buf_size: %d\n", path, offset, buf_size);
    file_entry file;
    cluster_t cluster = 0;
    off_t cluster_offset = 0;
    size_t size = 0;
    if (get_file_entry(path, &file) != 0) {
        return -ENOENT;
    }
    myfat_truncate(path, offset + buf_size);
    size = buf_size;
    size_t sum = 0;
    cluster = get_cluster_by_offset(file.cluster, offset);
    char *tmp = (char*)malloc(CLUSTER_SIZE);
    size_t current = 0;
    printf("size %d\n", size);
    while (sum < size) {
        printf("cluster: %d; sum: %d\n", cluster, sum);
        cluster_offset = offset % CLUSTER_SIZE;
        if (sum + CLUSTER_SIZE < size) {
            current = CLUSTER_SIZE - cluster_offset;
        } else {
            current = size - sum - cluster_offset;
        }
        printf("cluster_offset: %lld; current: %d\n", cluster_offset, current);
        lseek(image, data_offset + cluster * CLUSTER_SIZE + cluster_offset, SEEK_SET);
        memcpy(tmp, buf + sum, current);
        write(image, tmp, current);
        offset += current;
        sum += current;
        cluster = get_next_cluster(cluster);
    }
    free(tmp);
    printf("Finish file write; path: %s\n", path);
	return size;
}

int myfat_symlink(const char* to, const char* from) {
	mode_t mode = 0777;
	if (create_file(from, mode | S_IFLNK) != 0) {
		return -ENOENT;
	}
	myfat_write(from, to, strlen(to), 0, NULL);
	return 0;
}

int myfat_readlink(const char* path, char* buf, size_t size) {
	return myfat_read(path, buf, size, 0, NULL) ? 0 : 1;
}

struct fuse_operations myfat_operations = {
    .getattr    = myfat_getattr,
    .readdir    = myfat_readdir,
    .read       = myfat_read,
    .open 		= myfat_open,
    .mkdir  	= myfat_mkdir,
    .create     = myfat_create,
    .unlink     = myfat_unlink,
    .rmdir		= myfat_rmdir,
    .rename 	= myfat_rename,
    .truncate 	= myfat_truncate,
    .write      = myfat_write,
    .symlink	= myfat_symlink,
    .readlink	= myfat_readlink
};

int main(int argc, char *argv[]) {
	image = open(IMAGE_FILE_NAME, O_RDWR);
	if (image == -1) {
		printf("Error!\n");
		return 1;
	}
	data_offset = sizeof(cluster_t) * CLUSTER_COUNT;
	entries_count = CLUSTER_SIZE / sizeof(file_entry);
	return fuse_main(argc, argv, &myfat_operations, NULL);
}
