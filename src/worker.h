#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <grpc++/grpc++.h>
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::ShardInfo;
using masterworker::ShardComponent;
using masterworker::InterimFile;
using masterworker::OutputFile;
using masterworker::WorkerService;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
   This is a big task for this project, will test your understanding of map reduce */
extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/*
 * Worker serves Master's request via async RPC call.
 */
class Worker final : public WorkerService::Service {

public:
	/* DON'T change the function signature of this constructor */
	Worker(std::string ip_addr_port);

	/* DON'T change this function's signature */
	bool run();

private:
	std::string ip_addr_port;
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	Status DoMap(ServerContext *context, const ShardInfo *request,
		     InterimFile *reply) override {
		auto mapper = get_mapper_from_task_factory("cs6210");
		std::string filecontent;

		// debug
		std::cout << "Worker got fileshard info!\n";
		for (int i = 0; i < request->components_size(); i++) {
			const ShardComponent& component = request->components(i);
			const std::string& filename = component.filename();
			int start = component.start();
			int end = component.end();
			int size = component.size();

			// debug
			std::cout << "filename: " << filename;
			std::cout << ", start: " << start;
			std::cout << ", end: " << end;
			std::cout << ", size: " << size << std::endl;

			std::ifstream file(filename);
			if (file.is_open()) {
				file.seekg(start);
				filecontent.resize(size);
				file.read(&filecontent[0], size);
				std::stringstream ss(filecontent);
				std::string line;
				while (std::getline(ss, line)) {
					mapper->map(line);
				}
			}
		}
		std::vector<std::pair<std::string, std::string> >& vec = mapper->impl_->keyval_pair;
		sort(vec.begin(), vec.end());

		std::string interim_filename("worker_" + ip_addr_port);
		std::ofstream file(interim_filename);
		std::vector<std::pair<std::string, std::string> >::iterator it;
		if (file.is_open()) {
			for (it = vec.begin(); it != vec.end(); it++) {
				file << (*it).first << "," << (*it).second << "\n";
			}
			file.close();
		} else {
			std::cout << "failed to open file " << interim_filename << "\n";
		}

		reply->set_worker_addr(ip_addr_port);
		reply->set_filename(interim_filename);

		// std::vector<std::string>::iterator file_it;
		// for (file_it = mapper->impl_->interim_filenames.begin();
		//      file_it != mapper->impl_->interim_filenames.end(); file_it++)
		// 	reply->add_filenames(*file_it);

		return Status::OK;
	}

	Status DoReduce(ServerContext *context, const InterimFile *request,
		OutputFile *reply) override {
		auto reducer = get_reducer_from_task_factory("cs6210");
		std::ifstream file(request->filename());
		std::string line;

		std::string key_prev;
		std::vector<std::string> vals;
		while (std::getline(file, line)) {
			std::istringstream is_line(line);
			std::string key;
			if (std::getline(is_line, key, ',')) {
				std::string value;
				if (std::getline(is_line, value)) {
					if (key_prev.compare("") == 0 || key.compare(key_prev) == 0) {
						vals.push_back(value);
						key_prev = key;
					} else {
						reducer->reduce(key_prev, vals);
						key_prev = key;
						vals.clear();
						vals.push_back(value);
					}
				}
			}
		}
		std::vector<std::pair<std::string, std::string> >& vec = reducer->impl_->keyval_pair;
		std::string output_filename("./output/output_" + ip_addr_port);
		std::ofstream output_file(output_filename);
		std::vector<std::pair<std::string, std::string> >::iterator it;
		if (output_file.is_open()) {
			for (it = vec.begin(); it != vec.end(); it++) {
				output_file << (*it).first << "," << (*it).second << "\n";
			}
			output_file.close();
		} else {
			std::cout << "failed to open file " << output_filename << "\n";
		}
		reply->set_worker_addr(ip_addr_port);
		reply->set_filename(output_filename);

		return Status::OK;
		//reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	}
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
   You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) : ip_addr_port(ip_addr_port) {

}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
   from Master, complete when given one and again keep looking for the next one.
   Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
   BaseReduer's member BaseReducerInternal impl_ directly,
   so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	ServerBuilder builder;
	builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
	builder.RegisterService(this);

	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Worker listening on " << ip_addr_port << std::endl;

	server->Wait();
	return true;
}
