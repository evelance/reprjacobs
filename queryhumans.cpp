#include <iostream>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
using namespace std;

struct worker_info {
    pthread_t thread;
    unsigned id;
    uint8_t *data;
    size_t len;
    size_t humans_counted;
    uint64_t cnt[2][256][128];
};

void* worker(void* p)
{
    struct worker_info* info = (struct worker_info*)p;
    cout << "Thread #" << info->id << endl;
    for (uint8_t *p = info->data, *end = info->data + info->len; p < end; p += 16) {
        unsigned age =  (p[0]  & 0x7F);
        unsigned sex =  (p[0]  & 0x80) >> 7;
        unsigned ctry = (p[11] & 0xFF);
        info->cnt[sex][ctry][age]++;
        info->humans_counted++;
        if (info->humans_counted % 100000000 == 0)
            cout << "*" << flush;
    }
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        cout << "Benutzung: " << argv[0] << " Datenbankdatei Threads" << endl;
        return EXIT_FAILURE;
    }
    int nthreads = atoi(argv[2]);
    if (nthreads <= 0 || nthreads > 1000)
        nthreads = 1;
    cout << "Berechnung des Medianalters..." << endl;
    cout << "Datei: " << argv[1] << ", benutze Threads: " << nthreads << endl;
    // Map file into RAM with mmap
    int fd = open(argv[1], O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    cout << "Dateigröße: " << (st.st_size / (1024.0 * 1024.0 * 1024.0)) << "GB" << endl;
    uint8_t* data = (uint8_t*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        cout << "Kann Datei nicht in den Speicher mappen: " << strerror(errno) << endl;
        return EXIT_FAILURE;
    }
    if (madvise(data, st.st_size, MADV_SEQUENTIAL|MADV_WILLNEED|MADV_HUGEPAGE)) {
        cout << "Warnung: madvise: " << strerror(errno) << endl;
    }
    // Walk through data
    uint64_t humans = st.st_size / 16;
    uint64_t humans_per_thread = humans / nthreads;
    uint64_t humans_assigned = 0;
    cout << "Menschen in der Datei: " << humans << endl;
    struct worker_info* info = new struct worker_info[nthreads];
    memset(info, 0, sizeof (struct worker_info) * nthreads);
    cout << "Threads werden gestartet: " << nthreads << endl;
    for (int i = 0; i < nthreads; ++i) {
        info[i].id = i;
        info[i].humans_counted = 0;
        info[i].data = data + humans_assigned * 16;
        if (i + 1 < nthreads) {
            // Equal chunk of data for every thread
            info[i].len = humans_per_thread * 16;
            humans_assigned += humans_per_thread;
        } else {
            // Last thread has to do remaining work
            info[i].len = (humans - humans_assigned) * 16;
        }
        if (pthread_create(&info[i].thread, NULL, worker, &info[i])) {
            cout << "Kann Thread nicht starten: " << strerror(errno) << endl;
            return EXIT_FAILURE;
        }
    }
    for (int i = 0; i < nthreads; ++i) {
        pthread_join(info[i].thread, NULL);
    }
    cout << endl << "Threads beendet." << endl;
    // Sum up partial results
    uint64_t cnt[2][256][128] = {0};
    uint64_t sum_counted_humans = 0;
    for (int i = 0; i < nthreads; ++i) {
        sum_counted_humans += info[i].humans_counted;
        for (unsigned sex = 0; sex <= 1; ++sex)
            for (unsigned ctry = 0; ctry <= 255; ++ctry)
                for (unsigned age = 0; age <= 127; ++age)
                    cnt[sex][ctry][age] += info[i].cnt[sex][ctry][age];
    }
    cout << "Threads haben " << sum_counted_humans << " gezählt." << endl;
    cout << "Auflistung des Medianalters nach Geschlecht und Land" << endl;
    for (unsigned sex = 0; sex <= 1; ++sex) {
        cout << "Geschlecht: " << (sex ? "Männlich" : "Weiblich") << endl;
        for (unsigned ctry = 0; ctry <= 255; ++ctry) {
            int total = 0, acc = 0;
            for (unsigned age = 0; age <= 127; ++age)
                total += cnt[sex][ctry][age];
            for (unsigned age = 0; age <= 127; ++age) {
                acc += cnt[sex][ctry][age];
                if (acc >= (total / 2)) {
                    cout << "L" << ctry << ":" << age << "J ";
                    break;
                }
            }
        }
        cout << endl;
    }
    // Cleanup
    munmap(data, st.st_size);
    close(fd);
    cout << "Erledigt." << endl;
    return EXIT_SUCCESS;
}