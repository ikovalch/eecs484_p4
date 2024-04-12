#include "Join.hpp"

#include <iostream>
#include <unordered_map>
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel, pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
	// partitioning left
	// MEM_SIZE_IN_PAGE - 1 is 15
	//cout << "LEFT" << endl; //DEBUGGING

	for (uint page_id = left_rel.first; page_id < left_rel.second; ++page_id) {

		mem->loadFromDisk(disk, page_id, 0);
		auto tempPage = mem->mem_page(0);

		for (uint i = 0; i < tempPage->size(); ++i) {
			Record r = tempPage->get_record(i);
			uint bucket_idx = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1)
			        + 1; //do we have to account for the fact that this may be 0?
			auto assignmentPage = mem->mem_page(bucket_idx);

			if (assignmentPage->full()) { // once full, flush to disk
				uint disk_id = mem->flushToDisk(disk, bucket_idx);
				partitions[bucket_idx - 1].add_left_rel_page(disk_id); //disk id of that bucket page
			}

			assignmentPage->loadRecord(r); // put that record onto that page(loadRecord)

			//cout << bucket_idx << endl; //DEBUGGING
			//r.print();                  //DEBUGGING
		}
	}
	for (uint i = 1; i < MEM_SIZE_IN_PAGE - 1; i++) {
		if (!(mem->mem_page(i)->empty())) {
			uint disk_id = mem->flushToDisk(disk, i);
			partitions[i].add_left_rel_page(disk_id); //disk id of that bucket page
		}
	} //flush all of mem (that is not empty) to disk
	//mem->reset();

	// partitioning right
	//cout << "RIGHT" << endl; //DEBUGGING
	for (uint page_id = right_rel.first; page_id < right_rel.second; ++page_id) {

		mem->loadFromDisk(disk, page_id, 0); // reusing memory page 0
		Page* tempPage = mem->mem_page(0);   // get pointer to page 0

		for (uint i = 0; i < tempPage->size(); ++i) {
			Record r = tempPage->get_record(i);
			uint bucket_idx = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1) + 1;
			auto assignmentPage = mem->mem_page(bucket_idx);

			if (assignmentPage->full()) { // once full, flush to disk
				uint disk_id = mem->flushToDisk(disk, bucket_idx);
				partitions[bucket_idx - 1].add_right_rel_page(disk_id); //disk id of that bucket page
			}

			assignmentPage->loadRecord(r); // put that record onto that page(loadRecord)

			//cout << bucket_idx << endl; //DEBUGGING
			//r.print();                  //DEBUGGING
		}
	}

	for (uint i = 1; i < MEM_SIZE_IN_PAGE - 1; i++) {
		if (!(mem->mem_page(i)->empty())) {
			uint disk_id = mem->flushToDisk(disk, i);
			partitions[i].add_right_rel_page(disk_id); //disk id of that bucket page
		}
	} //flush all of mem (that is not empty) to disk

	//mem->reset();

	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder
	                         // let us assume we use the last page
	for (Bucket& bucket : partitions) {
		// unordered_map<uint, vector<Record>> hash_table;
		// check which side of the partition is smaller, hash table should get the smaller of the two
		// hash the left relation with h2
		vector<uint> value;
		if (bucket.get_left_rel().size() < bucket.get_right_rel().size()) {
			value = bucket.get_right_rel();
			for (uint page_id : bucket.get_left_rel()) {
				mem->loadFromDisk(disk, page_id, 0);      // mem page 0 for temp storage
				Page* page = mem->mem_page(0);            // get pointer to the page
				for (uint i = 0; i < page->size(); ++i) { // loop through all page records
					Record r = page->get_record(i);
					uint hash_value = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2) + 2;
					// hash_table[hash_value].push_back(r);
					mem->mem_page(hash_value)->loadRecord(r);
				}
			}
		} else {
			value = bucket.get_left_rel();
			for (uint page_id : bucket.get_right_rel()) {
				mem->loadFromDisk(disk, page_id, 0);      // mem page 0 for temp storage
				Page* page = mem->mem_page(0);            // get pointer to the page
				for (uint i = 0; i < page->size(); ++i) { // loop through all page records
					Record r = page->get_record(i);
					uint hash_value = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2) + 2;
					mem->mem_page(hash_value)->loadRecord(r);
				}
			}
		}
		// probe hash table with records from the opposite relation

		for (uint page_id : value) {
			mem->loadFromDisk(disk, page_id, 1); // mem page 1 for temp storage
			Page* page = mem->mem_page(1);
			for (uint i = 0; i < page->size(); ++i) {
				Record r = page->get_record(i);
				uint hash_value = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2) + 2;
				for (uint i = 0; i < mem->mem_page(hash_value)->size(); i++) {
					if (mem->mem_page(hash_value)->get_record(i) == r) {
						//cout << "JOIN THESE" << endl;
						// create or use a result page in memory to store the join result
						Page* resultPage = mem->mem_page(0);
						if (resultPage->full()) {
							//cout << "page is full" << endl;
							uint disk_id = mem->flushToDisk(disk, 0);
							disk_pages.push_back(disk_id);
						}
						resultPage->loadPair(mem->mem_page(hash_value)->get_record(i), r);
					}
				}
			}
		}
	}

	//gotta flush even if its not full
	if (!(mem->mem_page(0)->empty())) {
		//cout << "FLUSHED !" << endl;
		uint disk_id = mem->flushToDisk(disk, 0);
		disk_pages.push_back(disk_id);
	}
	// mem->reset();
	return disk_pages;
}
