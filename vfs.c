/*
  Big Brother File System
  Copyright (C) 2012 Joseph J. Pfeiffer, Jr., Ph.D. <pfeiffer@cs.nmsu.edu>

  This program can be distributed under the terms of the GNU GPLv3.
  See the file COPYING.

  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.
  A copy of that code is included in the file fuse.h
  
  The point of this FUSE filesystem is to provide an introduction to
  FUSE.  It was my first FUSE filesystem as I got to know the
  software; hopefully, the comments in this code will help people who
  follow later to get a gentler introduction.

  This might be called a no-op filesystem:  it doesn't impose
  filesystem semantics on top of any other existing structure.  It
  simply reports the requests that come in, and passes them to an
  underlying filesystem.  The information is saved in a logfile named
  bbfs.log, in the directory from which you run bbfs.

  gcc -Wall `pkg-config fuse --cflags --libs` -o bbfs bbfs.c
*/

#include "params.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include "log.h"
#include <openssl/md5.h>
#include <sys/stat.h>

struct vfs_state *vfs_data;

struct entry_s {
char *key;
char *value;
struct entry_s *next;
};

typedef struct entry_s entry_t;


struct hashtable_s {
int size;
struct entry_s **table;	
};

typedef struct hashtable_s hashtable_t;
hashtable_t *hashtable;

/* Create a new hashtable. */
hashtable_t *ht_create( int size ) {

hashtable_t *hashtable = NULL;
int i;

if( size < 1 ) return NULL;

/* Allocate the table itself. */
if( ( hashtable = malloc( sizeof( hashtable_t ) ) ) == NULL ) {
return NULL;
}

/* Allocate pointers to the head nodes. */
if( ( hashtable->table = malloc( sizeof( entry_t * ) * size ) ) == NULL ) {
return NULL;
}
for( i = 0; i < size; i++ ) {
hashtable->table[i] = NULL;
}

hashtable->size = size;

return hashtable;	
}

/* Hash a string for a particular hash table. */
int ht_hash( hashtable_t *hashtable, char *key ) {

unsigned long int hashval;
int i = 0;

/* Convert our string to an integer */
while( hashval < ULONG_MAX && i < strlen( key ) ) {
hashval = hashval << 8;
hashval += key[ i ];
i++;
}

return hashval % hashtable->size;
}

/* Create a key-value pair. */
entry_t *ht_newpair( char *key, char *value ) {
entry_t *newpair;

if( ( newpair = malloc( sizeof( entry_t ) ) ) == NULL ) {
return NULL;
}

if( ( newpair->key = strdup( key ) ) == NULL ) {
return NULL;
}

if( ( newpair->value = strdup( value ) ) == NULL ) {
return NULL;
}

newpair->next = NULL;

return newpair;
}

/* Insert a key-value pair into a hash table. */
void ht_set( hashtable_t *hashtable, char *key, char *value ) {
int bin = 0;
entry_t *newpair = NULL;
entry_t *next = NULL;
entry_t *last = NULL;

bin = ht_hash( hashtable, key );

next = hashtable->table[ bin ];

while( next != NULL && next->key != NULL && strcmp( key, next->key ) > 0 ) {
last = next;
next = next->next;
}

/* There's already a pair.  Let's replace that string. */
if( next != NULL && next->key != NULL && strcmp( key, next->key ) == 0 ) {

free( next->value );
next->value = strdup( value );

/* Nope, could't find it.  Time to grow a pair. */
} else {
newpair = ht_newpair( key, value );

/* We're at the start of the linked list in this bin. */
if( next == hashtable->table[ bin ] ) {
newpair->next = next;
hashtable->table[ bin ] = newpair;
/* We're at the end of the linked list in this bin. */
} else if ( next == NULL ) {
last->next = newpair;
/* We're in the middle of the list. */
} else  {
newpair->next = next;
last->next = newpair;
}
}
}

/* Retrieve a key-value pair from a hash table. */
char *ht_get( hashtable_t *hashtable, char *key ) {
int bin = 0;
entry_t *pair;

bin = ht_hash( hashtable, key );

/* Step through the bin, looking for our value. */
pair = hashtable->table[ bin ];
while( pair != NULL && pair->key != NULL && strcmp( key, pair->key ) > 0 ) {
pair = pair->next;
}

/* Did we actually find anything? */
if( pair == NULL || pair->key == NULL || strcmp( key, pair->key ) != 0 ) {
return NULL;

} else {
return pair->value;
}
}

// Report errors to logfile and give -errno to caller
static int vfs_error(char *str)
{
    int ret = -errno;
    
    log_msg("    ERROR %s: %s\n", str, strerror(errno));
    
    return ret;
}

// Check whether the given user is permitted to perform the given operation on the given 

//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void vfs_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, vfs_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
				    // break here

    log_msg("    vfs_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	    vfs_DATA->rootdir, path, fpath);
}

///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//
/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int vfs_getattr(const char *path, struct stat *statbuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_getattr(path=\"%s\", statbuf=0x%08x)\n",
	  path, statbuf);
    vfs_fullpath(fpath, path);
    
    retstat = lstat(fpath, statbuf);
    if (retstat != 0)
	retstat = vfs_error("vfs_getattr lstat");
    
    log_stat(statbuf);
    
    return retstat;
}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
// Note the system readlink() will truncate and lose the terminating
// null.  So, the size passed to to the system readlink() must be one
// less than the size passed to vfs_readlink()
// vfs_readlink() code by Bernardo F Costa (thanks!)
int vfs_readlink(const char *path, char *link, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("vfs_readlink(path=\"%s\", link=\"%s\", size=%d)\n",
	  path, link, size);
    vfs_fullpath(fpath, path);
    
    retstat = readlink(fpath, link, size - 1);
    if (retstat < 0)
	retstat = vfs_error("vfs_readlink readlink");
    else  {
	link[retstat] = '\0';
	retstat = 0;
    }
    
    return retstat;
}

/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int vfs_mknod(const char *path, mode_t mode, dev_t dev)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n",
	  path, mode, dev);
    vfs_fullpath(fpath, path);
    
    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        retstat = open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode);
	if (retstat < 0)
	    retstat = vfs_error("vfs_mknod open");
        else {
            retstat = close(retstat);
	    if (retstat < 0)
		retstat = vfs_error("vfs_mknod close");
	}
    } else
	if (S_ISFIFO(mode)) {
	    retstat = mkfifo(fpath, mode);
	    if (retstat < 0)
		retstat = vfs_error("vfs_mknod mkfifo");
	} else {
	    retstat = mknod(fpath, mode, dev);
	    if (retstat < 0)
		retstat = vfs_error("vfs_mknod mknod");
	}
    
    return retstat;
}

/** Create a directory */
int vfs_mkdir(const char *path, mode_t mode)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);
    vfs_fullpath(fpath, path);
    
    retstat = mkdir(fpath, mode);
    if (retstat < 0)
	retstat = vfs_error("vfs_mkdir mkdir");
    
    return retstat;
}

/** Remove a file */
int vfs_unlink(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("vfs_unlink(path=\"%s\")\n",
	    path);
    vfs_fullpath(fpath, path);
    
    retstat = unlink(fpath);
    if (retstat < 0)
	retstat = vfs_error("vfs_unlink unlink");
    
    return retstat;
}

/** Remove a directory */
int vfs_rmdir(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("vfs_rmdir(path=\"%s\")\n",
	    path);
    vfs_fullpath(fpath, path);
    
    retstat = rmdir(fpath);
    if (retstat < 0)
	retstat = vfs_error("vfs_rmdir rmdir");
    
    return retstat;
}

/** Create a symbolic link */
// The parameters here are a little bit confusing, but do correspond
// to the symlink() system call.  The 'path' is where the link points,
// while the 'link' is the link itself.  So we need to leave the path
// unaltered, but insert the link into the mounted directory.
int vfs_symlink(const char *path, const char *link)
{
    int retstat = 0;
    char flink[PATH_MAX];
    
    log_msg("\nvfs_symlink(path=\"%s\", link=\"%s\")\n",
	    path, link);
    vfs_fullpath(flink, link);
    
    retstat = symlink(path, flink);
    if (retstat < 0)
	retstat = vfs_error("vfs_symlink symlink");
    
    return retstat;
}

/** Rename a file */
// both path and newpath are fs-relative
int vfs_rename(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char fnewpath[PATH_MAX];
    
    log_msg("\nvfs_rename(fpath=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    vfs_fullpath(fpath, path);
    vfs_fullpath(fnewpath, newpath);
    
    retstat = rename(fpath, fnewpath);
    if (retstat < 0)
	retstat = vfs_error("vfs_rename rename");
    
    return retstat;
}

/** Create a hard link to a file */
int vfs_link(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX], fnewpath[PATH_MAX];
    
    log_msg("\nvfs_link(path=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    vfs_fullpath(fpath, path);
    vfs_fullpath(fnewpath, newpath);
    
    retstat = link(fpath, fnewpath);
    if (retstat < 0)
	retstat = vfs_error("vfs_link link");
    
    return retstat;
}

/** Change the permission bits of a file */
int vfs_chmod(const char *path, mode_t mode)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_chmod(fpath=\"%s\", mode=0%03o)\n",
	    path, mode);
    vfs_fullpath(fpath, path);
    
    retstat = chmod(fpath, mode);
    if (retstat < 0)
	retstat = vfs_error("vfs_chmod chmod");
    
    return retstat;
}

/** Change the owner and group of a file */
int vfs_chown(const char *path, uid_t uid, gid_t gid)
  
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_chown(path=\"%s\", uid=%d, gid=%d)\n",
	    path, uid, gid);
    vfs_fullpath(fpath, path);
    
    retstat = chown(fpath, uid, gid);
    if (retstat < 0)
	retstat = vfs_error("vfs_chown chown");
    
    return retstat;
}

/** Change the size of a file */
int vfs_truncate(const char *path, off_t newsize)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_truncate(path=\"%s\", newsize=%lld)\n",
	    path, newsize);
    vfs_fullpath(fpath, path);
    
    retstat = truncate(fpath, newsize);
    if (retstat < 0)
	vfs_error("vfs_truncate truncate");
    
    return retstat;
}

/** Change the access and/or modification times of a file */
/* note -- I'll want to change this as soon as 2.6 is in debian testing */
int vfs_utime(const char *path, struct utimbuf *ubuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_utime(path=\"%s\", ubuf=0x%08x)\n",
	    path, ubuf);
    vfs_fullpath(fpath, path);
    
    retstat = utime(fpath, ubuf);
    if (retstat < 0)
	retstat = vfs_error("vfs_utime utime");
    
    return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int vfs_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);
    vfs_fullpath(fpath, path);
    
    fd = open(fpath, fi->flags);
    if (fd < 0)
	retstat = vfs_error("vfs_open open");
    
    fi->fh = fd;
    log_fi(fi);
    
    return retstat;
}


char * substr(char * s, int x, int y)
{
    char * ret = malloc(strlen(s) + 1);
    char * p = ret;
    char * q = &s[x];

    while(x  < y)
    {
        *p++ = *q++;
        x ++;
    }

    *p++ = '\0';

    return ret;
}

char* get_md5_sum_formatted(char* md) {
	char* str = malloc((sizeof(char) * (MD5_DIGEST_LENGTH*2)) + 1);
	int i,j;
	for(i=0,j=0; i <MD5_DIGEST_LENGTH; i++) {
		char ch[3];
		sprintf(ch,"%02x",md[i]);
		str[j++] = ch[0];
		str[j++] = ch[1];
    }
    str[j] = '\0';
    return str;
}

int check_hash(const char *encrypted, const char *path) {
	char result[MD5_DIGEST_LENGTH];
	log_msg("-- In chech_hash --\n");
	MD5((unsigned char*) encrypted, sizeof(encrypted), result);
	char path_copy[strlen(path) + strlen(vfs_data->rootdir) + 1];
	strcpy(path_copy, vfs_data->rootdir);
	strcat(path_copy, path);
	char* hash = get_md5_sum_formatted(result);
	if(ht_get( hashtable, hash ) == NULL){
		log_msg(" \n--- Not found entry--- ");
		return 0;
	}else{
		log_msg(" \n--- Found entry--- ");
		return 1;
	}
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
// I don't fully understand the documentation above -- it doesn't
// match the documentation for the read() system call which says it
// can return with anything up to the amount of data requested. nor
// with the fusexmp code which returns the amount of data also
// returned by read.
int vfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int i, retstat = 0;
    
    log_msg("\nvfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
	    
    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0){
		retstat = vfs_error("vfs_read read");
		return retstat;
	}
    // Generate the hash
    char result[MD5_DIGEST_LENGTH] = {'\0'};
    MD5((unsigned char*) buf, sizeof(buf), result);
    char* hash = get_md5_sum_formatted((unsigned char*) result);
    
    // Read hash from file
    char path_copy[strlen(path) + strlen(vfs_data->rootdir) + strlen("_hash") + 1];
	strcpy(path_copy, vfs_data->rootdir);
	strcat(path_copy, path);
	strcat(path_copy, "_hash");
	log_msg(path_copy);
	// Get file Name
    for (i = strlen(path_copy); i >= 0; i--){
        if (path_copy[i] == '/') break;
    }
    char *folder = substr(path_copy, 0, i+1);
    char final_folder[strlen(folder) + strlen(".hash") + 1];
    strcpy(final_folder, folder);
    strcat(final_folder, ".hash");
    char *file_name = substr(path_copy, i + 1, strlen(path_copy));
    char final_file[strlen(final_folder) + strlen(file_name) + 2];
    strcpy(final_file, final_folder);
    strcat(final_file, "/");
    strcat(final_file, file_name);
    log_msg("\nFinal file path:");
    
    // Read from hash file
    FILE *stream;
    char *contents;
    int fileSize = 0;

    //Open the stream. Note "b" to avoid DOS/UNIX new line conversion.
    stream = fopen(final_file, "rb");

    //Seek to the end of the file to determine the file size
    fseek(stream, 0L, SEEK_END);
    fileSize = ftell(stream);
    fseek(stream, 0L, SEEK_SET);

    //Allocate enough memory (add 1 for the \0, since fread won't add it)
    contents = malloc(fileSize+1);

    //Read the file 
    size_t size1=fread(contents,1,fileSize,stream);
    contents[size1]=0; // Add terminating zero.

    //Print it again for debugging
    log_msg("%s", contents);

    //Close the file
    fclose(stream);
    log_msg("\nGenerated: ");
    log_msg(hash);
    log_msg("\nRead: ");
    log_msg(contents);
    log_msg("\n");
    if (strcmp(hash, contents) == 0){
		for (i = 0; i < size; ++i){
			buf[i] = buf[i] - 5;
		}
	}else{
		log_msg("\nHash did not match\n");
		char* val = ht_get(hashtable, contents);
		log_msg(val);
		for (i = 0; i < strlen(val); i++){
			if (val[i] == ':') break;
		}
		char* original_path = substr(val, 0, i);
		log_msg("\nOriginal Path: \n");
		log_msg(original_path);

		int fd;
		char fpath[PATH_MAX];
		//vfs_fullpath(fpath, original_path);
		log_msg("\nBefore Calling OPEN\n");
		fd = open(original_path, fi->flags);
		log_msg("\nAfter Calling OPEN\n");
		if (fd < 0){
			log_msg("fd is smaller than zero\n");
			retstat = vfs_error("vfs_open open");
		}
		retstat = pread(fd, buf, size, offset);
		for (i = 0; i < size; ++i){
			buf[i] = buf[i] - 5;
		}
		log_msg("\nRetstat: %d", retstat);
	}
	return retstat;
}


void write_hash(const char *encrypted, const char * path){
	int fd;
	char result[MD5_DIGEST_LENGTH] = {'\0'};
	MD5((unsigned char*) encrypted, sizeof(encrypted), result);
	char path_copy[strlen(path) + strlen(vfs_data->rootdir) + strlen("_hash") + 1];
	strcpy(path_copy, vfs_data->rootdir);
	strcat(path_copy, path);
	strcat(path_copy, "_hash");
	log_msg(path_copy);
	
	char* hash = get_md5_sum_formatted(result);
	log_msg(hash);
	//create hash directory if not exists
	int i;
    for (i = strlen(path_copy); i >= 0; i--){
        if (path_copy[i] == '/') break;
    }
    char *folder = substr(path_copy, 0, i+1);
    char final_folder[strlen(folder) + strlen(".hash") + 1];
    strcpy(final_folder, folder);
    strcat(final_folder, ".hash");
	
	struct stat st = {0};
	if (stat(final_folder, &st) == -1) {
		mkdir(final_folder, 0777);
	}
    char *file_name = substr(path_copy, i + 1, strlen(path_copy));
    char final_file[strlen(final_folder) + strlen(file_name) + 2];
    strcpy(final_file, final_folder);
    strcat(final_file, "/");
    strcat(final_file, file_name);
    log_msg("\nFinal file path:");
    log_msg(final_file);
    
    fd = open(final_file, O_RDWR | O_CREAT, 777);
	if (fd < 0) log_msg("\nError is opening hash file.");
	write(fd, hash, strlen(hash));
}

int vfs_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
    int i, retstat = 0;
    
    log_msg("\nvfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
	
    char *encrypted = malloc( sizeof(char) * (size) + 1);
    for (i = 0; i < size; i++){
		encrypted[i] = buf[i] + 5;
    }
    encrypted[i] = '\0';
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);
	
    /*retstat = pwrite(fi->fh, encrypted, size, offset);
    if (retstat < 0)
	retstat = vfs_error("vfs_write pwrite");*/
	
	// Dedup
	log_msg("Log for Dedup Start\n");
	char result[MD5_DIGEST_LENGTH] = {'\0'};
	MD5((unsigned char*) encrypted, sizeof(encrypted), result);
	char file_path[strlen(path) + strlen(vfs_data->rootdir) + 1];
	strcpy(file_path, vfs_data->rootdir);
	strcat(file_path, path);
	char* hash = get_md5_sum_formatted(result);
	if(check_hash(encrypted,path) == 0){
		ht_set( hashtable, hash, file_path );
		retstat = pwrite(fi->fh, encrypted, size, offset);
		if (retstat < 0) retstat = vfs_error("vfs_write pwrite");
	}else{
		char *val = ht_get(hashtable, hash);
		char newVal[strlen(val) + strlen(file_path) + 2];
		strcpy(newVal, val);
		strcat(newVal, ":");
		strcat(newVal, file_path);
		ht_set(hashtable, hash, newVal);
		retstat = pwrite(fi->fh, "", size, offset);
		if (retstat < 0) retstat = vfs_error("vfs_write pwrite");		
	}
	
	log_msg(ht_get( hashtable, hash )); 
	log_msg("\nLog for Dedup End\n");
	// Dedup
	
	write_hash(encrypted, path);
    return retstat;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int vfs_statfs(const char *path, struct statvfs *statv)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_statfs(path=\"%s\", statv=0x%08x)\n",
	    path, statv);
    vfs_fullpath(fpath, path);
    
    // get stats for underlying filesystem
    retstat = statvfs(fpath, statv);
    if (retstat < 0)
	retstat = vfs_error("vfs_statfs statvfs");
    
    log_statvfs(statv);
    
    return retstat;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int vfs_flush(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);
	
    return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int vfs_release(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_release(path=\"%s\", fi=0x%08x)\n",
	  path, fi);
    log_fi(fi);

    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    retstat = close(fi->fh);
    
    return retstat;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int vfs_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_fi(fi);
    
    // some unix-like systems (notably freebsd) don't have a datasync call
#ifdef HAVE_FDATASYNC
    if (datasync)
	retstat = fdatasync(fi->fh);
    else
#endif	
	retstat = fsync(fi->fh);
    
    if (retstat < 0)
	vfs_error("vfs_fsync fsync");
    
    return retstat;
}

#ifdef HAVE_SYS_XATTR_H
/** Set extended attributes */
int vfs_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
	    path, name, value, size, flags);
    vfs_fullpath(fpath, path);
    
    retstat = lsetxattr(fpath, name, value, size, flags);
    if (retstat < 0)
	retstat = vfs_error("vfs_setxattr lsetxattr");
    
    return retstat;
}

/** Get extended attributes */
int vfs_getxattr(const char *path, const char *name, char *value, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
	    path, name, value, size);
    vfs_fullpath(fpath, path);
    
    retstat = lgetxattr(fpath, name, value, size);
    if (retstat < 0)
	retstat = vfs_error("vfs_getxattr lgetxattr");
    else
	log_msg("    value = \"%s\"\n", value);
    
    return retstat;
}

/** List extended attributes */
int vfs_listxattr(const char *path, char *list, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char *ptr;
    
    log_msg("vfs_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
	    path, list, size
	    );
    vfs_fullpath(fpath, path);
    
    retstat = llistxattr(fpath, list, size);
    if (retstat < 0)
	retstat = vfs_error("vfs_listxattr llistxattr");
    
    log_msg("    returned attributes (length %d):\n", retstat);
    for (ptr = list; ptr < list + retstat; ptr += strlen(ptr)+1)
	log_msg("    \"%s\"\n", ptr);
    
    return retstat;
}

/** Remove extended attributes */
int vfs_removexattr(const char *path, const char *name)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_removexattr(path=\"%s\", name=\"%s\")\n",
	    path, name);
    vfs_fullpath(fpath, path);
    
    retstat = lremovexattr(fpath, name);
    if (retstat < 0)
	retstat = vfs_error("vfs_removexattr lrmovexattr");
    
    return retstat;
}
#endif

/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int vfs_opendir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp;
    int retstat = 0;
    char fpath[PATH_MAX];
    
    log_msg("\nvfs_opendir(path=\"%s\", fi=0x%08x)\n",
	  path, fi);
    vfs_fullpath(fpath, path);
    
    dp = opendir(fpath);
    if (dp == NULL)
	retstat = vfs_error("vfs_opendir opendir");
    
    fi->fh = (intptr_t) dp;
    
    log_fi(fi);
    
    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int vfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
    int retstat = 0;
    DIR *dp;
    struct dirent *de;
    
    log_msg("\nvfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
	    path, buf, filler, offset, fi);
    // once again, no need for fullpath -- but note that I need to cast fi->fh
    dp = (DIR *) (uintptr_t) fi->fh;

    // Every directory contains at least two entries: . and ..  If my
    // first call to the system readdir() returns NULL I've got an
    // error; near as I can tell, that's the only condition under
    // which I can get an error from readdir()
    de = readdir(dp);
    if (de == 0) {
	retstat = vfs_error("vfs_readdir readdir");
	return retstat;
    }

    // This will copy the entire directory into the buffer.  The loop exits
    // when either the system readdir() returns NULL, or filler()
    // returns something non-zero.  The first case just means I've
    // read the whole directory; the second means the buffer is full.
    do {
	log_msg("calling filler with name %s\n", de->d_name);
	if (filler(buf, de->d_name, NULL, 0) != 0) {
	    log_msg("    ERROR vfs_readdir filler:  buffer full");
	    return -ENOMEM;
	}
    } while ((de = readdir(dp)) != NULL);
    
    log_fi(fi);
    
    return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int vfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_releasedir(path=\"%s\", fi=0x%08x)\n",
	    path, fi);
    log_fi(fi);
    
    closedir((DIR *) (uintptr_t) fi->fh);
    
    return retstat;
}

/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
// when exactly is this called?  when a user calls fsync and it
// happens to be a directory? ???
int vfs_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_fi(fi);
    
    return retstat;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
// Undocumented but extraordinarily useful fact:  the fuse_context is
// set up before this function is called, and
// fuse_get_context()->private_data returns the user_data passed to
// fuse_main().  Really seems like either it should be a third
// parameter coming in here, or else the fact should be documented
// (and this might as well return void, as it did in older versions of
// FUSE).
void *vfs_init(struct fuse_conn_info *conn)
{
    log_msg("\nvfs_init()\n");
    
    log_conn(conn);
    log_fuse_context(fuse_get_context());
    
    return vfs_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void vfs_destroy(void *userdata)
{
    log_msg("\nvfs_destroy(userdata=0x%08x)\n", userdata);
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int vfs_access(const char *path, int mask)
{
    int retstat = 0;
    char fpath[PATH_MAX];
   
    log_msg("\nvfs_access(path=\"%s\", mask=0%o)\n",
	    path, mask);
    vfs_fullpath(fpath, path);
    
    retstat = access(fpath, mask);
    
    if (retstat < 0)
	retstat = vfs_error("vfs_access access");
    
    return retstat;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int vfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    int fd;
    
    log_msg("\nvfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
	    path, mode, fi);
    vfs_fullpath(fpath, path);
    
    fd = creat(fpath, mode);
    if (fd < 0)
	retstat = vfs_error("vfs_create creat");
    
    fi->fh = fd;
    
    log_fi(fi);
    
    return retstat;
}

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int vfs_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n",
	    path, offset, fi);
    log_fi(fi);
    
    retstat = ftruncate(fi->fh, offset);
    if (retstat < 0)
	retstat = vfs_error("vfs_ftruncate ftruncate");
    
    return retstat;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int vfs_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_msg("\nvfs_fgetattr(path=\"%s\", statbuf=0x%08x, fi=0x%08x)\n",
	    path, statbuf, fi);
    log_fi(fi);

    // On FreeBSD, trying to do anything with the mountpoint ends up
    // opening it, and then using the FD for an fgetattr.  So in the
    // special case of a path of "/", I need to do a getattr on the
    // underlying root directory instead of doing the fgetattr().
    if (!strcmp(path, "/"))
	return vfs_getattr(path, statbuf);
    
    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
	retstat = vfs_error("vfs_fgetattr fstat");
    
    log_stat(statbuf);
    
    return retstat;
}

struct fuse_operations vfs_oper = {
  .getattr = vfs_getattr,
  .readlink = vfs_readlink,
  // no .getdir -- that's deprecated
  .getdir = NULL,
  .mknod = vfs_mknod,
  .mkdir = vfs_mkdir,
  .unlink = vfs_unlink,
  .rmdir = vfs_rmdir,
  .symlink = vfs_symlink,
  .rename = vfs_rename,
  .link = vfs_link,
  .chmod = vfs_chmod,
  .chown = vfs_chown,
  .truncate = vfs_truncate,
  .utime = vfs_utime,
  .open = vfs_open,
  .read = vfs_read,
  .write = vfs_write,
  /** Just a placeholder, don't set */ // huh???
  .statfs = vfs_statfs,
  .flush = vfs_flush,
  .release = vfs_release,
  .fsync = vfs_fsync,
  
#ifdef HAVE_SYS_XATTR_H
  .setxattr = vfs_setxattr,
  .getxattr = vfs_getxattr,
  .listxattr = vfs_listxattr,
  .removexattr = vfs_removexattr,
#endif
  
  .opendir = vfs_opendir,
  .readdir = vfs_readdir,
  .releasedir = vfs_releasedir,
  .fsyncdir = vfs_fsyncdir,
  .init = vfs_init,
  .destroy = vfs_destroy,
  .access = vfs_access,
  .create = vfs_create,
  .ftruncate = vfs_ftruncate,
  .fgetattr = vfs_fgetattr
};

void vfs_usage()
{
    fprintf(stderr, "usage:  bbfs [FUSE and mount options] rootDir mountPoint\n");
    abort();
}

int main(int argc, char *argv[])
{
    int fuse_stat;
    hashtable = ht_create( 65536 );
    
    // bbfs doesn't do any access checking on its own (the comment
    // blocks in fuse.h mention some of the functions that need
    // accesses checked -- but note there are other functions, like
    // chown(), that also need checking!).  Since running bbfs as root
    // will therefore open Metrodome-sized holes in the system
    // security, we'll check if root is trying to mount the filesystem
    // and refuse if it is.  The somewhat smaller hole of an ordinary
    // user doing it with the allow_other flag is still there because
    // I don't want to parse the options string.
    if ((getuid() == 0) || (geteuid() == 0)) {
	fprintf(stderr, "Running BBFS as root opens unnacceptable security holes\n");
	return 1;
    }
    
    // Perform some sanity checking on the command line:  make sure
    // there are enough arguments, and that neither of the last two
    // start with a hyphen (this will break if you actually have a
    // rootpoint or mountpoint whose name starts with a hyphen, but so
    // will a zillion other programs)
    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
	vfs_usage();

    vfs_data = malloc(sizeof(struct vfs_state));
    if (vfs_data == NULL) {
	perror("main calloc");
	abort();
    }

    // Pull the rootdir out of the argument list and save it in my
    // internal data
    vfs_data->rootdir = realpath(argv[argc-2], NULL);
    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;
    
    vfs_data->logfile = log_open();
    
    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");    
    fuse_stat = fuse_main(argc, argv, &vfs_oper, vfs_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
    
    return fuse_stat;
}
