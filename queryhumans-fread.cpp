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

int main(int argc, char **argv)
{
    if (argc < 2) {
        cout << "Benutzung: " << argv[0] << " Datenbankdatei" << endl;
        return EXIT_FAILURE;
    }
    // Get file size
    int fd = open(argv[1], O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    cout << "Dateigröße: " << (st.st_size / (1024.0 * 1024.0 * 1024.0)) << "GB" << endl;
    close(fd);
    // Open file and walk through data with fread+buffer
    FILE* f = fopen(argv[1], "rb");
    if (f == NULL) {
        cout << "Fehler: " << strerror(errno) << endl;
        return EXIT_FAILURE;
    }
    size_t stsize = st.st_size;
    size_t cnt[2][256][128];
    size_t humans_counted = 0;
    const size_t bufsize = 4 * 1024;
    uint8_t* buffer = new uint8_t[bufsize];
    for (size_t done = 0, chunk; done < stsize; done += chunk) {
        chunk = bufsize;
        if (done + chunk > stsize) {
            chunk = stsize - done;
            if (chunk % 16 != 0)
                chunk = chunk - chunk % 16;
        }
        size_t r = fread(buffer, chunk, 1, f);
        if (r != 1) {
            cout << "Fehler: Kann nicht genug Daten lesen: " << strerror(errno) << endl;
            return EXIT_FAILURE;
        }
        for (uint8_t *p = buffer, *end = buffer + chunk; p < end; p += 16) {
            unsigned age =  (p[0]  & 0x7F);
            unsigned sex =  (p[0]  & 0x80) >> 7;
            unsigned ctry = (p[11] & 0xFF);
            cnt[sex][ctry][age]++;
            humans_counted++;
            if (humans_counted % 100000000 == 0)
                cout << "*" << flush;
        }
    }
    delete[] buffer;
    fclose(f);
    // Print results
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
    cout << "Erledigt." << endl;
    return EXIT_SUCCESS;
}