#pragma once

#include <vector>
#include <fstream>
#include <math.h>
#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

#define KB 1024
#define minimum(A, B) (A) < (B) ? (A) : (B)

/* file shard component - a shard contains one or more shard components, each identified by filename and file offset */
struct shardcmp {
	std::string filename;
	std::streampos start; /* starting location of shard component */
	std::streampos end; /* ending location of shard component */
	int offset; /* end - start */
};

struct FileShard {
	int id;
	std::vector<shardcmp *> shard_cmps;
};

inline void shardcmp_print(shardcmp *cmp)
{
	std::cout << "\tshard component:\n";
	std::cout << "\tfilename: " << cmp->filename << "\n";
	std::cout << "\tstart: " << cmp->start << ", ";
	std::cout << "end: " << cmp->end << ", ";
	std::cout << "offset: " << cmp->offset << "\n";
}

inline void fileshard_print(const FileShard& fs)
{
	std::cout << "File shard id: " << fs.id << "\n";
	std::vector<shardcmp *>::const_iterator it;
	for (it = fs.shard_cmps.begin(); it != fs.shard_cmps.end(); it++)
		shardcmp_print(*it);
}

inline void fileshard_add_cmps(FileShard *fs, const std::vector<shardcmp *>& shard_cmps)
{
	fs->shard_cmps = shard_cmps;
}

inline FileShard *fileshard_create(int id)
{
	FileShard *shard;

	shard = new FileShard;
	shard->id = id;
	return shard;
}

inline shardcmp *shardcmp_create(const std::string& filename, std::streampos start, std::streampos end)
{
	shardcmp *cmp;

	cmp = new shardcmp;
	cmp->filename = filename;
	cmp->start = start;
	cmp->end = end;
	cmp->offset = end - start;
	return cmp;
}

/* From the current char position, find the previous newline char */
inline void adjust(int pos)
{

}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int shard_size = mr_spec.shard_size * KB;
	std::vector<infile *>::const_iterator it;
	int fileshard_id = 0;
	int nshards, remain;
	int start, end;
	int i;
	int filesize;

	FileShard *fileshard = NULL;
	shardcmp *shard_cmp = NULL;
	std::vector<shardcmp *> shard_cmps;
	int need_more = 0;
	// std::cout << "shard size: " << shard_size << "\n";
	for (it = mr_spec.in_files.begin(); it != mr_spec.in_files.end(); it++) {
		filesize = (*it)->size;
		// std::cout << "file name: " << (*it)->filename << ", size: " << filesize
		// 	  << ", need_more: " << need_more << "\n";
		if (need_more > 0) {
			if (need_more <= filesize) {
				shard_cmp = shardcmp_create((*it)->filename, 0, need_more);
				// //debug
				// shardcmp_print(shard_cmp);
				shard_cmps.push_back(shard_cmp);
				filesize -= need_more;
				fileshard_add_cmps(fileshard, shard_cmps);
				// // debug
				// fileshard_print(*fileshard);

				fileShards.push_back(*fileshard);
				shard_cmps.clear();
				need_more = 0;
			} else {
				shard_cmp = shardcmp_create((*it)->filename, 0, filesize);
				// //debug
				// shardcmp_print(shard_cmp);
				shard_cmps.push_back(shard_cmp);
				need_more -= filesize;
				// std::cout << "need_more: " << need_more << "\n";
			}
		}
		if (need_more == 0) {
			nshards = filesize / shard_size;
			remain = filesize % shard_size;
			start = 0;
			end = minimum(start + shard_size, filesize);
			for (i = 0; i < nshards; i++) {
				fileshard = fileshard_create(++fileshard_id);
				shard_cmp = shardcmp_create((*it)->filename, start, end);
				// //debug
				// shardcmp_print(shard_cmp);
				shard_cmps.push_back(shard_cmp);
				fileshard_add_cmps(fileshard, shard_cmps);
				fileShards.push_back(*fileshard);
				shard_cmps.clear();
				start = (i + 1) * shard_size;
				end = start + shard_size;
				// // debug
				// fileshard_print(*fileshard);
			}
			if (remain > 0) {
				need_more = shard_size - remain;
				fileshard = fileshard_create(++fileshard_id);
				shard_cmp = shardcmp_create((*it)->filename, start, end);
				// //debug
				// shardcmp_print(shard_cmp);
				shard_cmps.push_back(shard_cmp);
				// // debug
				// fileshard_print(*fileshard);
			}
		}
	}
	if (need_more > 0) {
		fileshard_add_cmps(fileshard, shard_cmps);
		// // debug
		// fileshard_print(*fileshard);
		fileShards.push_back(*fileshard);
	}

	return true;
}

// inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
// 	std::vector<infile *> in_files;
// 	std::vector<infile *>::iterator fp;
// 	struct shardcmp *shardcmp;
// 	struct FileShard *fs;
// 	int toread;
// 	int shard_id;
// 	std::streampos curr;
// 	int infile_cnt;
// 	int shard_size; /* config-specified shard size in bytes */

// 	in_files = mr_spec.in_files;
// 	fp = in_files.begin();
// 	shard_id = 0;
// 	infile_cnt = 0;
// 	shard_size = mr_spec.shard_size * 1024;
// 	while (infile_cnt < in_files.size() && (*fp)->avail > 0) {
// 		// std::cout << "[debug] filename: " << (*fp)->filename << "\n";
// 		// std::cout << "[debug] avail: " << (*fp)->avail << "\n";
// 		// std::cout << "[debug] pos: " << (*fp)->pos << "\n";
// 		fs = fileshard_create(shard_id++);
// 		toread = shard_size;
// 		while (toread && infile_cnt < in_files.size()) {
// 			// std::cout << "[debug] infile size: " << in_files.size() << "\n";
// 			// std::cout << "[debug] infile count: " << infile_cnt << "\n";
// 			if (toread < (*fp)->avail) {
// 				std::ifstream file((*fp)->filename);
// 				file.seekg((*fp)->pos);
// 				file.seekg(toread, std::ios::cur);
// 				curr = file.tellg();
// 				//adjust();
// 				(*fp)->avail -= ((*fp)->pos - curr);
// 				shardcmp = shardcmp_create((*fp)->filename, (*fp)->pos, curr);
// 				(*fp)->pos = curr;
// 				fs->shard_cmps.push_back(shardcmp);
// 				break;
// 			} else {
// 				std::ifstream file((*fp)->filename);
// 				file.seekg((*fp)->pos);
// 				file.seekg((*fp)->avail, std::ios::cur);
// 				curr = file.tellg();
// 				toread -= (*fp)->avail;
// 				// std::cout << "toread left: " << toread << "\n";
// 				shardcmp = shardcmp_create((*fp)->filename, (*fp)->pos, curr);
// 				(*fp)->avail = 0;
// 				(*fp)->pos = curr;
// 				fs->shard_cmps.push_back(shardcmp);
// 				fp++;
// 				infile_cnt++;
// 			}
// 		}
// 		fileShards.push_back(*fs);
// 		fileshard_print(fs);
// 	}
// 	return true;
// }
