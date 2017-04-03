#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <map>
#include <grpc++/grpc++.h>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include "workerpool.h"
#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::Status;
using masterworker::ShardInfo;
using masterworker::ShardComponent;
using masterworker::InterimFile;
using masterworker::OutputFile;
using masterworker::WorkerService;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
   This is probably the biggest task for this project, will test your understanding of map reduce */

/* Idea: master maintains a queue of workers that are ready.
 * If the queue is empty, Master blocks until a worker is done with its work.
 * This is implemented as a solution to producer-consuer problem with condition variables.
 * When a GRPC call returns, it adds the associated worker to the ready queue.
 */

/* Master makes client rpc requests to workers */
class Master {

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec&, const std::vector<FileShard>&);

	/* DON'T change this function's signature */
	bool run();

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	const MapReduceSpec& mr_spec;
	const std::vector<FileShard>& file_shards;
	//std::vector<std::unique_ptr<Worker::Stub> > stubs;
	std::map<std::string, std::unique_ptr<WorkerService::Stub> > addr_to_stub;
	std::vector<std::string> interim_filenames;
	std::vector<std::string> final_filenames;

	std::queue<std::string> ready_worker_queue;
	std::mutex m;
	std::condition_variable cv;
	CompletionQueue cq;
	std::thread response_listener;
	std::thread response_listener_reduce;

	struct AsyncClientCall {
		ClientContext context;
		InterimFile reply;
		Status status;
		std::unique_ptr<ClientAsyncResponseReader<InterimFile> > response_reader;
	};
	bool mapping_done;

	struct AsyncClientCall_reduce {
		ClientContext context;
		OutputFile reply;
		Status status;
		std::unique_ptr<ClientAsyncResponseReader<OutputFile> > response_reader;
	};
	bool reduce_done;

	void DoMap(const std::string&, const FileShard&);
	void DoReduce(std::string&, std::string&);
	void AsyncCompleteRpc();
	void AsyncCompleteRpc_reduce();
};

/* CS6210_TASK: This is all the information your master will get from the framework.
   You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
	: mr_spec(mr_spec), file_shards(file_shards)
{
	mapping_done = false;
	reduce_done = false;
	//fileshard_print(file_shards);
	/* create a channel betwen master and the workers */
	for (auto &worker_addr : mr_spec.worker_ipaddr_ports) {
		std::shared_ptr<Channel> channel =
			grpc::CreateChannel(worker_addr, grpc::InsecureChannelCredentials());
		std::unique_ptr<WorkerService::Stub> stub(WorkerService::NewStub(channel));
		//stubs.push_back(std::move(stub));
		addr_to_stub.insert(std::pair<std::string, std::unique_ptr<WorkerService::Stub> >(worker_addr, std::move(stub)));
		ready_worker_queue.push(worker_addr);
	}
	//workers = new Workerpool(mr_spec.n_workers);
	response_listener = std::thread(&Master::AsyncCompleteRpc, this);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	/* make workers do the map work until all input shards are processed */
	std::vector<FileShard>::const_iterator fileshard_it;

	//debug
	for (fileshard_it = file_shards.begin(); fileshard_it != file_shards.end(); fileshard_it++) {
		fileshard_print(*fileshard_it);
	}

	for (fileshard_it = file_shards.begin(); fileshard_it != file_shards.end(); fileshard_it++) {
		std::unique_lock<std::mutex> lk(m);
		while (ready_worker_queue.size() == 0)
			cv.wait(lk, [this]{return ready_worker_queue.size() > 0;});
		std::string& ready_worker = ready_worker_queue.front();
		ready_worker_queue.pop();
		DoMap(ready_worker, *fileshard_it);
		lk.unlock();
	}
	mapping_done = true;
	// std::cout << "ready Q size: " << ready_worker_queue.size() << "\n";
	response_listener.join();
	std::cout<< "mapper thread joined\n";

	/* once all mapping is done, start the reduce work */
	std::vector<std::string>::iterator interim_it;
	response_listener_reduce = std::thread(&Master::AsyncCompleteRpc_reduce, this);

	for (interim_it = interim_filenames.begin(); interim_it != interim_filenames.end(); interim_it++) {
		std::unique_lock<std::mutex> lk(m);
		while (ready_worker_queue.size() == 0)
			cv.wait(lk, [this]{return ready_worker_queue.size() > 0;});
		std::string& ready_worker = ready_worker_queue.front();
		ready_worker_queue.pop();
		DoReduce(ready_worker, *interim_it);
		lk.unlock();
	}
	reduce_done = true;

	response_listener_reduce.join();
	std::cout << "reducer thread joined\n";

	return true;
}

/* if response comes back from server, enqueues the worker to the ready worker queue */
void Master::AsyncCompleteRpc()
{
	void *got_tag;
	bool ok = false;
	//GPR_ASSERT(cq.Next(&worker_tag, &ok)); /* blocks until next result is available in cq */
	while (!mapping_done && cq.Next(&got_tag, &ok)) { // this can be buggy
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		//GPR_ASSERT(worker_tag == (void *) worker.id);
		GPR_ASSERT(ok);
		/* when master hears back from worker (via RPC call), add that worker to the
		 * ready queue, so that it can be scheduled to process next shard
		 */
		//workers->add_ready_worker(worker);
		std::unique_lock<std::mutex> lk(m);
		ready_worker_queue.push(call->reply.worker_addr());
		// std::cout << "debug " << call->reply.worker_addr() << "\n";
		// std::cout << "ready Q size: " << ready_worker_queue.size() << "\n";
		cv.notify_one();
		lk.unlock();

		if (!call->status.ok()) {
			std::cout << call->status.error_code() << ": " << call->status.error_message()
				  << std::endl;
			return;
		}

		interim_filenames.push_back(call->reply.filename());
		//std::cout << "deb " << call->reply.filename() << "\n";
		//interim_filenames.push_back(call->reply.filenames());
		delete call;
	}
}

/* async RPC request to worker - maybe the consumer() function in class Workerpool should use this instead? */
void Master::DoMap(const std::string& worker, const FileShard& fileshard)
{
	ShardInfo request;
	ShardComponent *component;

	request.set_id(fileshard.id);
	std::vector<shardcmp *>::const_iterator cmp_it;
	for (cmp_it = fileshard.shard_cmps.begin(); cmp_it != fileshard.shard_cmps.end(); cmp_it++) {
		component = request.add_components();
		component->set_filename((*cmp_it)->filename);
		component->set_start((*cmp_it)->start);
		component->set_end((*cmp_it)->end);
		component->set_size((*cmp_it)->offset);
	}

	std::unique_ptr<WorkerService::Stub>& stub_ = addr_to_stub.at(worker);

	AsyncClientCall *call = new AsyncClientCall;
	call->response_reader = stub_->AsyncDoMap(&call->context, request, &cq);
	call->response_reader->Finish(&call->reply, &call->status, (void*) call);
	// std::unique_ptr<ClientAsyncResponseReader<InterimFile> > rpc(
	// 	stub_->AsyncDoMap(&context, request, &cq));
	// rpc->Finish(&reply, &status, (void *) );
}

void Master::AsyncCompleteRpc_reduce()
{
	void *got_tag;
	bool ok = false;
	while (!reduce_done && cq.Next(&got_tag, &ok)) {
		AsyncClientCall_reduce* call = static_cast<AsyncClientCall_reduce*>(got_tag);
		GPR_ASSERT(ok);
		std::unique_lock<std::mutex> lk(m);
		ready_worker_queue.push(call->reply.worker_addr());
		cv.notify_one();
		lk.unlock();

		if (!call->status.ok()) {
			std::cout << call->status.error_code() << ": " << call->status.error_message()
				  << std::endl;
			return;
		}
		final_filenames.push_back(call->reply.filename());
		delete call;
	}
}

void Master::DoReduce(std::string& worker, std::string& interim_filename)
{
	InterimFile request;
	request.set_filename(interim_filename);
	std::unique_ptr<WorkerService::Stub>& stub_ = addr_to_stub.at(worker);
	AsyncClientCall_reduce *call = new AsyncClientCall_reduce;
	call->response_reader = stub_->AsyncDoReduce(&call->context, request, &cq);
	call->response_reader->Finish(&call->reply, &call->status, (void *) call);
}
