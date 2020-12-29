// querryTestNativ.cpp : Diese Datei enthält die Funktion "main". Hier beginnt und endet die Ausführung des Programms.
//

#include <windows.h>
#include <iostream>
#include <tchar.h>

#include <stdint.h>
#define HAVE_STRUCT_TIMESPEC
#include <pthread.h>

using namespace std;

struct worker_info {
	pthread_t thread;
	unsigned id;
	uint8_t* data;
	size_t len;
	size_t humans_counted;
	uint64_t cnt[2][256][128];
};

void* worker(void* p)
{
	struct worker_info* info = (struct worker_info*)p;
	cout << "Thread #" << info->id << endl;
	for (uint8_t* p = info->data, *end = info->data + info->len; p < end; p += 16) {
		unsigned age = (p[0] & 0x7F);
		unsigned sex = (p[0] & 0x80) >> 7;
		unsigned ctry = (p[11] & 0xFF);
		info->cnt[sex][ctry][age]++;
		info->humans_counted++;
		if (info->humans_counted % 100000000 == 0)
			cout << "*" << flush;
	}
	return NULL;
}

int main(int argc, char** argv)
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
	// Map file into RAM with windows

	HANDLE hMapFile;      // handle for the file's memory-mapped region
	HANDLE hFile;         // the file handle
	BOOL bFlag;           // a result holder
	DWORD dBytesWritten;  // number of bytes written
	DWORD dwFileMapSize;  // size of the file mapping
	DWORD dwMapViewSize;  // the size of the view
	DWORD dwFileMapStart; // where to start the file map view
	DWORD dwSysGran;      // system allocation granularity
	SYSTEM_INFO SysInfo;  // system information; used to get granularity
	LPVOID lpMapAddress;  // pointer to the base address of the
						  // memory-mapped region
	char* pData;         // pointer to the data
	int i;                // loop counter
	int iData;            // on success contains the first int of data
	int iViewDelta;       // the offset into the view where the data
						  //shows up

	//const char*  file  = "C:\ws\queryTestNativ\querryTestNativ\Debug\database.db";
	//const TCHAR* lpcTheFile = TEXT("C:/ws/queryTestNativ/querryTestNativ/Debug/database.db");
	//const TCHAR* lpcTheFile = TEXT("C:/Users/JOJO/Desktop/INM/INM2/Trends/database.db");
	const TCHAR* lpcTheFile = TEXT("bigdata.dat");

	hFile = CreateFile(lpcTheFile,
		GENERIC_READ,
		FILE_SHARE_READ,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED,
		NULL);

	DWORD error = GetLastError();
	printf("GetLastError (Number): %d, ", error);

	if (error == ERROR_FILE_NOT_FOUND)
	{
		printf("File Not found \n");
	}

	if (hFile == INVALID_HANDLE_VALUE)
	{
		
		_tprintf(TEXT("hFile is NULL\n"));
		_tprintf(TEXT("Target file is %s\n"),
			lpcTheFile);
		return 4;
	}
	LARGE_INTEGER size;
	GetFileSizeEx(hFile, &size);

	cout << "File size... "<< size.QuadPart/1024.0/1024.0/1024.0 << endl;
	cout << "Size_t size... " << sizeof(size_t) << endl;


	hMapFile = CreateFileMapping(hFile,          // current file handle
		NULL,           // default security
		PAGE_READONLY, // read/write permission
		0,              // size of mapping object, high
		0,  // size of mapping object, low
		NULL);          // name of mapping object

	if (hMapFile == NULL)
	{
		_tprintf(TEXT("hMapFile is NULL: last error: %d\n"), GetLastError());
		return (2);
	}

	lpMapAddress = MapViewOfFile(hMapFile,            // handle to
												// mapping object
		FILE_MAP_READ,      // read/write
		0,                  // high-order 32 // bits of file // offset
		0,                  // low-order 32 bits of file offset
		0); 
	// number of bytes to map
	if (lpMapAddress == NULL)
	{
		_tprintf(TEXT("lpMapAddress is NULL: last error: %d\n"), GetLastError());
		return 3;
	}

	uint8_t* data = (uint8_t*)lpMapAddress;
	//cout << "Dateigröße: " << (st.st_size / (1024.0 * 1024.0 * 1024.0)) << "GB" << endl;



		// Walk through data
	uint64_t humans = size.QuadPart / 16;
	uint64_t humans_per_thread = humans / nthreads;
	uint64_t humans_assigned = 0;
	cout << "Menschen in der Datei: " << humans << endl;
	struct worker_info* info = new struct worker_info[nthreads];
	memset(info, 0, sizeof(struct worker_info) * nthreads);
	cout << "Threads werden gestartet: " << nthreads << endl;
	for (int i = 0; i < nthreads; ++i) {
		info[i].id = i;
		info[i].humans_counted = 0;
		info[i].data = data + humans_assigned * 16;
		if (i + 1 < nthreads) {
			// Equal chunk of data for every thread
			info[i].len = humans_per_thread * 16;
			humans_assigned += humans_per_thread;
		}
		else {
			// Last thread has to do remaining work
			info[i].len = (humans - humans_assigned) * 16;
		}
		if (pthread_create(&info[i].thread, NULL, worker, &info[i])) {
			cout << "Kann Thread nicht starten... " << endl;
			return EXIT_FAILURE;
		}
	}
	for (int i = 0; i < nthreads; ++i) {
		pthread_join(info[i].thread, NULL);
	}
	cout << endl << "Threads beendet." << endl;
	// Sum up partial results
	//uint64_t* cnt = new uint64_t[2*256*128];
	//memset(cnt, 0, sizeof(uint64_t) * 2*256*128);
	uint64_t cnt[2][256][128] = { 0 };
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
	bFlag = UnmapViewOfFile(lpMapAddress);
	bFlag = CloseHandle(hMapFile); // close the file mapping object

	if (!bFlag)
	{
		_tprintf(TEXT("\nError %ld occurred closing the mapping object!"),
			GetLastError());
	}

	bFlag = CloseHandle(hFile);   // close the file itself

	if (!bFlag)
	{
		_tprintf(TEXT("\nError %ld occurred closing the file!"),
			GetLastError());
	}
	cout << "Erledigt." << endl;
	return EXIT_SUCCESS;
}

// Programm ausführen: STRG+F5 oder "Debuggen" > Menü "Ohne Debuggen starten"
// Programm debuggen: F5 oder "Debuggen" > Menü "Debuggen starten"

// Tipps für den Einstieg: 
//   1. Verwenden Sie das Projektmappen-Explorer-Fenster zum Hinzufügen/Verwalten von Dateien.
//   2. Verwenden Sie das Team Explorer-Fenster zum Herstellen einer Verbindung mit der Quellcodeverwaltung.
//   3. Verwenden Sie das Ausgabefenster, um die Buildausgabe und andere Nachrichten anzuzeigen.
//   4. Verwenden Sie das Fenster "Fehlerliste", um Fehler anzuzeigen.
//   5. Wechseln Sie zu "Projekt" > "Neues Element hinzufügen", um neue Codedateien zu erstellen, bzw. zu "Projekt" > "Vorhandenes Element hinzufügen", um dem Projekt vorhandene Codedateien hinzuzufügen.
//   6. Um dieses Projekt später erneut zu öffnen, wechseln Sie zu "Datei" > "Öffnen" > "Projekt", und wählen Sie die SLN-Datei aus.
