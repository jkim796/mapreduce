#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <vector>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */

/* input file */
struct infile {
	std::string filename;
	int size; /* file size in bytes */
	// std::streampos pos; /* current position */
	// int avail; /* availalbe space left in bytes */
};

struct MapReduceSpec {
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<infile *> in_files;
	std::string output_dir;
	int n_output_files; /* basically R */
	int shard_size; /* in KB */
	std::string user_id;
};

inline int get_fsize(const std::string& filename)
{
	std::streampos begin, end;
	std::ifstream myfile(filename.c_str());

	begin = myfile.tellg();
	myfile.seekg(0, std::ios::end);
	end = myfile.tellg();
	myfile.close();
	//std::cout << "[debug] file size: " << (end - begin) << " bytes.\n";
	return end - begin;
}

inline void mrspec_fill(MapReduceSpec &mr_spec, const std::string& key, const std::string& value)
{
	struct infile *input_file;
	// std::cout << "filling out mrspec\n";
	// std::cout << "key: " << key << " value: " << value << "\n";
	if (key.compare("n_workers") == 0) {
		mr_spec.n_workers = std::stoi(value);
	} else if (key.compare("worker_ipaddr_ports") == 0) {
		std::stringstream ss(value);
		std::string addr;
		while (std::getline(ss, addr, ',')) {
			mr_spec.worker_ipaddr_ports.push_back(addr);
		}
	} else if (key.compare("input_files") == 0) {
		std::stringstream ss(value);
		std::string filename;
		while (std::getline(ss, filename, ',')) {
			// std::cout << filename << "\n";
			input_file = new infile;
			input_file->filename = filename;
			input_file->size = get_fsize(filename);
			// input_file->avail = get_fsize(filename);
			// input_file->pos = 0;
			mr_spec.in_files.push_back(input_file);
		}
	} else if (key.compare("output_dir") == 0) {
		mr_spec.output_dir = value;
	} else if (key.compare("n_output_files") == 0) {
		mr_spec.n_output_files = std::stoi(value);
	} else if (key.compare("map_kilobytes") == 0) {
		mr_spec.shard_size = std::stoi(value);
	} else if (key.compare("user_id") == 0) {
		mr_spec.user_id = value;
	}
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream config_file(config_filename);
	// std::cout << "reading config file\n";
	if (config_file.is_open()) {
		std::string line;
		while (std::getline(config_file, line)) {
			std::istringstream is_line(line);
			std::string key;
			if (std::getline(is_line, key, '=')) {
				std::string value;
				if (std::getline(is_line, value)) {
					// std::cout << "key: " << key << " value: " << value << "\n";
					mrspec_fill(mr_spec, key, value);
				}
			}
		}
		config_file.close();
		return true;
	} else {
		std::cerr << "Failed to open file " << config_filename << std::endl;
		return false;
	}
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	return true;
}
