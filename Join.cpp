#include "Join.hpp"

#include <vector>

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
        auto tempPage = mem->loadFromDisk(disk, page_id, 0);
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
	return disk_pages;
}
