#include "Join.hpp"

#include <vector>
#include <unordered_map>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk)); 
	// Partitioning the left relation
    for (uint page_id = left_rel.first; page_id <= left_rel.second; ++page_id) {
        mem->reset();
		mem->loadFromDisk(disk, page_id, 0);
		auto tempPage = mem->mem_page(0);
        for (uint i = 0; i < tempPage->size(); ++i) {
            Record r = tempPage->get_record(i);
            uint bucket_idx = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_idx].add_left_rel_page(page_id);
        }
    }
	// Partitioning the right relation
    for (uint page_id = right_rel.first; page_id <= right_rel.second; ++page_id) {
        mem->loadFromDisk(disk, page_id, 0); // Reusing memory page 0
        Page* tempPage = mem->mem_page(0);
        for (uint i = 0; i < tempPage->size(); ++i) {
            Record r = tempPage->get_record(i);
            uint bucket_idx = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            partitions[bucket_idx].add_right_rel_page(page_id);
        }
    }

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
        unordered_map<uint, vector<Record>> hash_table; // In-memory hash table

        // Building hash table for left relation records in this bucket
        for (uint page_id : bucket.get_left_rel()) {
            mem->loadFromDisk(disk, page_id, 0); // Assuming memory page 0 is used for temporary storage
            Page* page = mem->mem_page(0);
            for (uint i = 0; i < page->size(); ++i) {
                Record r = page->get_record(i);
                uint hash_value = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                hash_table[hash_value].push_back(r);
            }
        }
		  // Probing hash table with records from the right relation
        for (uint page_id : bucket.get_right_rel()) {
            mem->loadFromDisk(disk, page_id, 1); // Assuming memory page 1 is used here
            Page* page = mem->mem_page(1);
            for (uint i = 0; i < page->size(); ++i) {
                Record r = page->get_record(i);
                uint hash_value = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                if (hash_table.find(hash_value) != hash_table.end()) {
                    for (Record& left_record : hash_table[hash_value]) {
                        if (left_record == r) {
                            // When a match is found, create or use a result page in memory to store the join result
                            // You might need to flush this page to disk if it's full, and then start a new page
                            // Keep track of the disk page IDs where the join results are stored
							Page* resultPage = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
							if (resultPage->full()){
								uint disk_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
								disk_pages.push_back(disk_id);
							}
							resultPage->loadPair(left_record, r);
                        }
                    }
                }
            }
        }
    }
	return disk_pages;
}
