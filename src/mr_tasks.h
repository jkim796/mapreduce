#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>

/* CS6210_TASK Implement this data structureas per your implementation.
   You will need this when your worker is running the map task*/
struct BaseMapperInternal {

	/* DON'T change this function's signature */
	BaseMapperInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	std::vector<std::pair<std::string, std::string> > keyval_pair;
};

/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << ", " << this << std::endl;

	keyval_pair.push_back(std::make_pair(key, val));
	// int filenum;
	// filenum = hash(key) % r;
	// std::ofstream file("somefile", std::ios::app);
	// if (file.is_open()) {
	// 	file << keyvalpair;
	// 	file.close();
	// } else {
	// 	std::cout << "Error occured!\n";
	// }
}

/*-----------------------------------------------------------------------------------------------*/

/* CS6210_TASK Implement this data structureas per your implementation.
   You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

	/* DON'T change this function's signature */
	BaseReducerInternal();

	/* DON'T change this function's signature */
	void emit(const std::string& key, const std::string& val);

	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	std::vector<std::pair<std::string, std::string> > keyval_pair;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;

	keyval_pair.push_back(std::make_pair(key, val));
}
