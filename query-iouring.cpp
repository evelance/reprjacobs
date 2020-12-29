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
#include <assert.h>
#include <liburing.h>
using namespace std;

#define QD	8
#define BS	(16*1024)

/* Read request context */
struct readreq {
    off_t offset; /* offset in input file */
    off_t wanted; /* amount of data we want to read */
    off_t current; /* what we got so far */
    struct iovec iov; /* single iovec with offset/length for buffer */
    uint8_t buffer[BS];
};

/* Add readv request to submission queue */
static int queue_readv(struct io_uring *ring, int fd, off_t size, off_t offset)
{
    assert(size >= 0);
    assert(size <= BS);
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (! sqe)
        return 1;
    struct readreq* rr = new struct readreq;
    rr->offset = offset;
    rr->wanted = size;
    rr->current = 0;
    rr->iov.iov_base = rr->buffer;
    rr->iov.iov_len  = size;
    io_uring_prep_readv(sqe, 0, &rr->iov, 1, offset);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, rr);
    return 0;
}

static void queue_again(struct io_uring *ring, int fd, struct readreq *rr)
{
    struct io_uring_sqe *sqe;
    sqe = io_uring_get_sqe(ring);
    assert(sqe);
    io_uring_prep_readv(sqe, 0, &rr->iov, 1, rr->offset);
    sqe->flags |= IOSQE_FIXED_FILE;
    io_uring_sqe_set_data(sqe, rr);
}

int main(int argc, char **argv)
{
    /* Required parameters: input file name and number of threads */
    if (argc < 3) {
        cout << "Benutzung: " << argv[0] << " Datenbankdatei Threads" << endl;
        return EXIT_FAILURE;
    }
    int nthreads = atoi(argv[2]);
    if (nthreads <= 0 || nthreads > 1000) {
        cout << "Fehler: Gegebene Anzahl Threads ist ungültig" << endl;
        return EXIT_FAILURE;
    }
    cout << "Berechnung des Medianalters..." << endl;
    cout << "Datei: " << argv[1] << ", benutze Threads: " << nthreads << endl;
    /* Open input file and get size */
    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        cout << "Fehler: " << strerror(errno) << endl;
        return EXIT_FAILURE;
    }
    struct stat st;
    fstat(fd, &st);
    uint64_t filesize = st.st_size;
    cout << "Dateigröße: " << (filesize / (1024.0 * 1024.0 * 1024.0)) << "GB" << endl;
    /* Init IO system */
    if (geteuid()) {
        fprintf(stderr, "You need root privileges to run this program.\n");
        return 1;
    }
    int ret;
    struct io_uring ring;
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 2000;
    ret = io_uring_queue_init_params(QD, &ring, &params);
    if (ret < 0) {
        fprintf(stderr, "Fehler: queue_init: %s\n", strerror(-ret));
        return EXIT_FAILURE;
    }
    int fds[1];
    fds[0] = fd;
    ret = io_uring_register_files(&ring, fds, 1);
    if (ret) {
        fprintf(stderr, "Fehler: io_uring_register_files: %s", strerror(-ret));
        return EXIT_FAILURE;
    }
    /* Walk through data */
    uint64_t cnt[2][256][128];
    size_t humans = filesize / 16;
    size_t data_remaining = humans * 16;
    size_t read_off = 0;
    size_t data_end = data_remaining;
    size_t pending_requests = 0;
    while (data_remaining) {
        /* Fill submission queue with read requests */
        size_t has_requests = pending_requests;
        while (read_off < data_end && pending_requests < QD) {
            size_t chunk = data_end - read_off;
            if (chunk > BS)
                chunk = BS;
            //cout << "rr: off=" << read_off << ", len=" << chunk << endl;
            if (queue_readv(&ring, fd, chunk, read_off))
                break; /* Submission queue full */
            read_off += chunk;
            ++pending_requests;
        }
        /* Submit new requests */
        if (has_requests != pending_requests) {
            ret = io_uring_submit(&ring);
            if (ret < 0) {
                fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
                return EXIT_FAILURE;
            }
        }
        //cout << "pending=" << pending_requests << ", rem=" << data_remaining << endl;
        /* Handle completions */
        while (pending_requests) {
            struct io_uring_cqe *cqe;
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
                return EXIT_FAILURE;
            }
            if (!cqe)
                break;
            struct readreq* rr = (struct readreq*)io_uring_cqe_get_data(cqe);
            if (cqe->res <= 0) {
                fprintf(stderr, "Error: readv failed: %s\n", strerror(-cqe->res));
                return EXIT_FAILURE;
            } else {
                rr->current += cqe->res;
                if ((size_t)cqe->res < rr->iov.iov_len) {
                    /* Short read, adjust and requeue */
                    rr->iov.iov_base = (uint8_t*)rr->iov.iov_base + cqe->res;
                    rr->iov.iov_len -= cqe->res;
                    rr->offset += cqe->res;
                    fprintf(stderr,
                        "Warning: short read: got %d, have=%zu, rem=%zu off=%zu\n",
                        cqe->res, rr->current, rr->iov.iov_len, rr->offset);
                    io_uring_cqe_seen(&ring, cqe);
                    queue_again(&ring, fd, rr);
                    ret = io_uring_submit(&ring);
                    if (ret < 0) {
                        fprintf(stderr, "io_uring_submit: %s\n", strerror(-ret));
                        return EXIT_FAILURE;
                    }
                    continue;
                }
            }
            /* Read completed */
            data_remaining -= rr->current;
            --pending_requests;
            /* Process data */
            for (uint8_t *p = rr->buffer, *end = rr->buffer + rr->current; p < end; p += 16) {
                unsigned age =  (p[0]  & 0x7F);
                unsigned sex =  (p[0]  & 0x80) >> 7;
                unsigned ctry = (p[11] & 0xFF);
                cnt[sex][ctry][age]++;
            }
            /* Free request resources */
            delete rr;
            io_uring_cqe_seen(&ring, cqe);
        }
    }
    /* Print results */
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
    /* Cleanup */
    io_uring_queue_exit(&ring);
    close(fd);
    cout << "Erledigt." << endl;
    return EXIT_SUCCESS;
}

