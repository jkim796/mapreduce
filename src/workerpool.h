#pragma once

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

/*
 * This is how Master manages worker processes. It has a queue of worker threads,
 * and Master assigns a shard to the head of the queue. If the queue is empty,
 * it will block until a worker is done with its work and added back to the queue.
 * This mechanism is essentially an implementation of a solution to the producer
 * consumer problem using condition variables.
 */
class Workerpool {
public:
	Workerpool(int n, void (*func)(void *)) : nthreads(n) {
		std::cout << "Creating a threadpool with " << n << " threads.\n";
		threadwork = func;
		for (int i = 0; i < n; i++) {
			threads.push_back(std::thread(&Workerpool::consumer, this));
		}
	}

	void consumer() {
		while (true) {
			std::unique_lock<std::mutex> lk(m);
			std::cout << "consumer(): queuesize = " << queue.size() << "\n";
			while (queue.size() == 0)
				cv.wait(lk, [this]{return queue.size() > 0;});
			void *tag = queue.front();
			queue.pop();
			threadwork(tag);
			lk.unlock();
		}
	}

	/* producer thread - adds to the queue */
	void add_ready_worker(void *tag) {
		std::unique_lock<std::mutex> lk(m);
		queue.push(tag);
		cv.notify_one();
		lk.unlock();
	}

private:
	std::vector<std::thread> threads;
	int nthreads;
	std::mutex m;
	std::condition_variable cv;
	std::queue<void *> queue; /* queue of ready workers */
	std::function<void(void *)> threadwork;
};
