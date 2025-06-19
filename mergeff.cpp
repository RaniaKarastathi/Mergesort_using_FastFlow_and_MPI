#include <ff/ff.hpp>
#include <iostream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <cstdlib>
#include <ctime>
#include <chrono>

using namespace ff;

#ifndef RPAYLOAD
#define RPAYLOAD 64
#endif

struct Record {
    unsigned long key;
    char rpayload[RPAYLOAD];
};

static Record* generateRandomArray(int n) {
    srand(time(0));
    Record* records = new Record[n];
    for (int i = 0; i < n; i++) {
        records[i].key = rand();
        memset(records[i].rpayload, 'A', RPAYLOAD);
    }
    return records;
}

static bool isArraySorted(Record* a, int n) {
    for (int i = 1; i < n; i++) {
        if (a[i].key < a[i - 1].key)
            return false;
    }
    return true;
}

struct Task {
    Record* array;
    int left, right;
};
struct Result {
    Record* array;
    int left, right;
};

class Emitter : public ff_node {
public:
    Emitter(Record* arr, int n, int chunkSize)
        : arr(arr), n(n), chunkSize(chunkSize) {}

    void* svc(void*) override {
        for (int i = 0; i < n; i += chunkSize) {
            Task* t = new Task{arr, i, std::min(i + chunkSize - 1, n - 1)};
            ff_send_out(t);
        }
        return EOS;
    }

private:
    Record* arr;
    int n;
    int chunkSize;
};

class Worker : public ff_node {
public:
    void* svc(void* task) override {
        Task* t = static_cast<Task*>(task);
        std::sort(&t->array[t->left], &t->array[t->right + 1],
                  [](const Record& a, const Record& b) {
                      return a.key < b.key;
                  });
        Result* res = new Result{t->array, t->left, t->right};
        delete t;
        return res;
    }
};
class Collector : public ff_node {
public:
    Collector(int n, int chunkSize) : total(n), chunkSize(chunkSize) {}

    void* svc(void* res) override {
        Result* r = static_cast<Result*>(res);
        results.push_back(*r);
        delete r;

        if ((int)results.size() == expectedParts()) {
            std::vector<Result>* resultsCopy = new std::vector<Result>(results);
            return resultsCopy;
        }
        return GO_ON;
    }

private:
    int total;
    int chunkSize;
    std::vector<Result> results;

    int expectedParts() const {
        return (total + chunkSize - 1) / chunkSize;
    }
};
class AllToAllMerger : public ff_node {
public:
    bool isSorted = false;

    void* svc(void* task) override {
        std::vector<Result>* chunks = static_cast<std::vector<Result>*>(task);

        std::sort(chunks->begin(), chunks->end(),
                  [](const Result& a, const Result& b) {
                      return a.left < b.left;
                  });

        int total = 0;
        for (const auto& c : *chunks)
            total += (c.right - c.left + 1);

        Record* mergedArray = new Record[total];

        for (const auto& part : *chunks) {
            memcpy(mergedArray + part.left, part.array + part.left,
                   (part.right - part.left + 1) * sizeof(Record));
        }

        int start = chunks->at(0).left;
        for (size_t i = 1; i < chunks->size(); i++) {
            int next_start = chunks->at(i).left;
            int next_end = chunks->at(i).right;

            std::inplace_merge(mergedArray + start, mergedArray + next_start, mergedArray + next_end + 1,
                [](const Record& a, const Record& b) { return a.key < b.key; });

        }

        isSorted = isArraySorted(mergedArray, total);
        delete[] mergedArray;
        delete chunks;

        return EOS;
    }
};
int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "Usage: " << argv[0] << " -s <num_elements> -r <payload_size> -t <num_threads>\n";
        return 1;
    }

    int N = 0, T = 0, R = 64;

    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "-s") N = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "-r") R = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "-t") T = std::atoi(argv[++i]);
    }

    if (R != RPAYLOAD) {
        std::cerr << "Error: compile with -DRPAYLOAD=" << R << "\n";
        return 1;
    }


    std::unique_ptr<Record[]> array(generateRandomArray(N));
    int chunkSize = (N + T - 1) / T;

    
    Emitter emitter(array.get(), N, chunkSize);
    Collector collector(N, chunkSize);
    AllToAllMerger merger;

    std::vector<ff_node*> workers;
    for (int i = 0; i < T; i++)
        workers.push_back(new Worker());  

    ff_farm farm;
    farm.add_emitter(&emitter);
    farm.add_workers(workers);
    farm.add_collector(&collector);

    ff_pipeline pipe;
    pipe.add_stage(&farm);
    pipe.add_stage(&merger);

    auto start = std::chrono::high_resolution_clock::now();

    if (pipe.run_and_wait_end() < 0) {
        std::cerr << "Error running pipeline.\n";
        return 1;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

    std::cout << "Sorted: " << (merger.isSorted ? "YES" : "NO") << "\n";
    std::cout << "Elapsed Time: " << elapsed_sec << " sec\n";


    for (auto w : workers) delete w;

    return 0;
}


