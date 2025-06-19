#include <ff/ff.hpp>
#include <mpi.h>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <iostream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>

using namespace ff;

#ifndef RPAYLOAD
#define RPAYLOAD 64
#endif

struct Record {
    unsigned long key;
    char rpayload[RPAYLOAD];
};

bool isArraySorted(Record* a, int n) {
    for (int i = 1; i < n; i++)
        if (a[i].key < a[i - 1].key) return false;
    return true;
}

Record* generateRandomArray(int n) {
    srand(time(0));
    Record* records = new Record[n];
    for (int i = 0; i < n; i++) {
        records[i].key = rand();
        memset(records[i].rpayload, 'A', RPAYLOAD);
    }
    return records;
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
    Emitter(Record* arr, int n, int chunkSize) : arr(arr), n(n), chunkSize(chunkSize) {}

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
        std::sort(&t->array[t->left], &t->array[t->right + 1], [](const Record& a, const Record& b) {
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
    AllToAllMerger(Record* outputArray, int totalSize) : outputArray(outputArray), total(totalSize) {}

    void* svc(void* task) override {
        std::vector<Result>* chunks = static_cast<std::vector<Result>*>(task);

        std::sort(chunks->begin(), chunks->end(),
                  [](const Result& a, const Result& b) { return a.left < b.left; });

        for (const auto& part : *chunks) {
            memcpy(outputArray + part.left, part.array + part.left, (part.right - part.left + 1) * sizeof(Record));
        }

        for (size_t i = 1; i < chunks->size(); ++i) {
            std::inplace_merge(outputArray,
                               outputArray + chunks->at(i).left,
                               outputArray + chunks->at(i).right + 1,
                               [](const Record& a, const Record& b) { return a.key < b.key; });
        }

        delete chunks;
        return EOS;
    }

private:
    Record* outputArray;
    int total;
};
void runFastFlowSort(Record* array, int size, int numThreads) {
    int chunkSize = (size + numThreads - 1) / numThreads;
    std::vector<ff_node*> workers;
    for (int i = 0; i < numThreads; ++i)
        workers.push_back(new Worker());
    
    Emitter emitter(array, size, chunkSize);
    Collector collector(size, chunkSize);
    AllToAllMerger merger(array, size);

    ff_farm farm;
    farm.add_emitter(&emitter);
    farm.add_workers(workers);
    farm.add_collector(&collector);

    ff_pipeline pipe;
    pipe.add_stage(&farm);
    pipe.add_stage(&merger);

    if (pipe.run_and_wait_end() < 0) {
        exit(1);
    }

    for (auto w : workers) delete w;

}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided < MPI_THREAD_MULTIPLE) {
        std::cerr << "MPI does not support MPI_THREAD_MULTIPLE, provided level = " << provided << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int N = 0, R = 64, T = 4;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-s") N = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "-r") R = std::atoi(argv[++i]);
        else if (std::string(argv[i]) == "-t") T = std::atoi(argv[++i]);
    }

    if (R != RPAYLOAD) {
        if (rank == 0)
            std::cerr << "Compile with -DRPAYLOAD=" << R << "\n";
        MPI_Finalize();
        return 1;
    }

    int numWorkers = size - 2;
    int chunkSize = (N + numWorkers - 1) / numWorkers;

    if (rank == 0) {
        Record* fullArray = generateRandomArray(N);
        std::vector<MPI_Request> sendRequests(numWorkers);

        for (int i = 1; i <= numWorkers; ++i) {
            int start = (i - 1) * chunkSize;
            int end = std::min(i * chunkSize, N);
            int count = end - start;
            MPI_Isend(fullArray + start, count * sizeof(Record), MPI_BYTE, i, 0, MPI_COMM_WORLD, &sendRequests[i - 1]);
        }

        MPI_Waitall(numWorkers, sendRequests.data(), MPI_STATUSES_IGNORE);
        delete[] fullArray;

    } else if (rank == size - 1) {
        std::vector<Record*> chunks(numWorkers);
        std::vector<int> sizes(numWorkers);
        std::vector<MPI_Request> recvRequests(numWorkers);
        int total = 0;

        auto start_time = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < numWorkers; ++i) {
            int start = i * chunkSize;
            int end = std::min((i + 1) * chunkSize, N);
            int count = end - start;
            sizes[i] = count;
            chunks[i] = new Record[count];
            MPI_Irecv(chunks[i], count * sizeof(Record), MPI_BYTE, i + 1, 0, MPI_COMM_WORLD, &recvRequests[i]);
            total += count;
        }

        MPI_Waitall(numWorkers, recvRequests.data(), MPI_STATUSES_IGNORE);

        std::vector<Record> mergedArray;
        for (int i = 0; i < numWorkers; ++i) {
            mergedArray.insert(mergedArray.end(), chunks[i], chunks[i] + sizes[i]);
            delete[] chunks[i];
        }

        std::sort(mergedArray.begin(), mergedArray.end(),
          [](const Record& a, const Record& b) { return a.key < b.key; });

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end_time - start_time;

        bool sorted = isArraySorted(mergedArray.data(), total);
        std::cout << (sorted ? "Sort: YES" : "Sort: NO")
                  << " | Elapsed Time: " << elapsed.count() << " sec\n";
                   } else {
        int start = (rank - 1) * chunkSize;
        int end = std::min(rank * chunkSize, N);
        int count = end - start;

        Record* localArray = new Record[count];
        MPI_Recv(localArray, count * sizeof(Record), MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        runFastFlowSort(localArray, count, T);

        MPI_Send(localArray, count * sizeof(Record), MPI_BYTE, size - 1, 0, MPI_COMM_WORLD);
        delete[] localArray;
    }

    MPI_Finalize();
    return 0;
}

