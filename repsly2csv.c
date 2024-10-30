#define _GNU_SOURCE
#define _XOPEN_SOURCE

#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <json-c/json.h>
#include <json-c/json_object.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>

/* Constants */
#define MAX_URL_LENGTH 512
#define MAX_BUFFER 16384
#define MAX_DATE_LENGTH 64
#define MAX_ERROR_LENGTH 256
#define DEFAULT_RATE_LIMIT_SECONDS 1
#define DEFAULT_PAGE_SIZE 50
#define DEFAULT_RETRY_ATTEMPTS 3
#define DEFAULT_TIMEOUT 30
#define DEFAULT_MAX_ITERATIONS 1000
#define DEFAULT_WINDOW_SIZE 60  
#define DEFAULT_MAX_REQUESTS 60 
#define MIN_REQUEST_INTERVAL 1  
#define MAX_THREADS 6
#define MAX_QUEUE_SIZE 12 

volatile sig_atomic_t shutdown_requested = 0;

/* Configuration Structure */
typedef struct {
    int debug_mode;
    int raw_data_mode;
    int verbose_mode;
    char *specific_endpoint;
    char *output_directory;
    int rate_limit;
    int page_size;
    char *from_date;
    char *to_date;
    int retry_attempts;
    int timeout;
    int use_cache;
    int update_cache;
    char *export_format;
    char *log_file;
    int max_iterations;
} AppConfig;

typedef struct {
    char *memory;
    size_t size;
} MemoryStruct;

typedef enum {
    NONE,
    TIMESTAMP,
    ID,
    SKIP,
    DATE_RANGE
} PaginationType;

typedef struct {
    int records_processed;
    int total_records;
    char last_value[64];
    int page_number;
    bool has_more;
    time_t last_timestamp;
} PaginationState;

typedef struct {
    char **headers;
    int header_count;
    char *last_row_values;
    size_t last_row_size;
} CSVState;

typedef struct {
    const char *name;
    const char *key;
    PaginationType pagination_type;
    const char *url_format;
    bool use_raw_timestamp;
    bool use_timestamp_pagination;
    bool include_inactive;
    bool include_deleted;
    int max_page_size;
    const char *required_parameters;
    int default_date_range_days;
    bool requires_auth;
} Endpoint;

typedef struct {
    char message[MAX_ERROR_LENGTH];
    int code;
    const char *endpoint;
    const char *url;
} ErrorInfo;

typedef struct {
    time_t last_request;       
    int requests_in_window;    
    int window_size;           
    int max_requests;          
    double min_interval;       
    bool backoff_active;       
    int backoff_multiplier;    
} RateLimiter;

typedef struct {
    Endpoint* endpoint;
    char* output_directory;
    bool completed;
    ErrorInfo error;
    char* temp_filename;
} Job;

typedef struct {
    Job* jobs[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int size;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} JobQueue;

typedef struct {
    pthread_t threads[MAX_THREADS];
    JobQueue job_queue;
    ThreadSafeRateLimiter* rate_limiter;
    bool shutdown;
    int active_threads;
    pthread_mutex_t thread_count_mutex;
} ThreadPool;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t rate_limit_cv;
    time_t last_request;
    int requests_in_window;
    int window_size;
    int max_requests;
    double min_interval;
    bool backoff_active;
    int backoff_multiplier;
    double avg_response_time;
    int active_requests;
    int total_requests;
} ThreadSafeRateLimiter;

typedef struct {
    char *source_file;
    char *target_file;
    bool is_first;
    const char *format;
} FileMergeTask;

typedef struct {
    ThreadPool *pool;
    bool active;
} MonitorData;

typedef enum {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARNING,
    LOG_ERROR
} LogLevel;

typedef struct {
    char url[MAX_URL_LENGTH];
    MemoryStruct chunk;
    char last_id[64];
    PaginationState pagination;
    int skip;
    char *begin_date;
    char *end_date;
    FILE *output_file;
    CSVState csv_state;
    bool first_batch;
    int result;
    int records_processed;  // Add this to track records
} ProcessingContext;

typedef struct {
    bool inArray;
    bool skipBrackets;
    int bracketDepth;
} JSONMergeContext;

typedef struct {
    const Endpoint *endpoint;
    Job **jobs;
    int count;
} EndpointGroup;

/* Globals */
CURL *curl_handle;
AppConfig config = {0};
FILE *log_file_ptr = NULL;

pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t curl_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Function Declarations */

/* Core Application Functions */
void initializeApplication(void);
void cleanupApplication(void);
void handleCriticalError(const char *message, ThreadPool *pool);
void handleSignal(int signum);

/* Thread Pool Management */
int initializeThreadPool(ThreadPool* pool, int numThreads);
void cleanupThreadPool(ThreadPool* pool);
void* workerThreadFunction(void* arg);
void* monitorThreadFunction(void* arg);

/* Job Management */
Job* createJob(Endpoint* endpoint, const char* outputDir);
void destroyJob(Job* job);
bool enqueueJob(JobQueue* queue, Job* job);
Job* dequeueJob(JobQueue* queue);

/* Rate Limiting */
void initializeRateLimiter(ThreadSafeRateLimiter* limiter);
void applyRateLimit(ThreadSafeRateLimiter* limiter, ErrorInfo* error);
void handleRateLimitResponse(ThreadSafeRateLimiter* limiter, int httpStatus, int responseTime);

/* Data Processing */
int processEndpoint(const Endpoint *endpoint, const char *tempFilename, 
                   ThreadSafeRateLimiter *rateLimiter, ErrorInfo *error);
int fetchData(const char* url, MemoryStruct* chunk, CURL *threadCurl,
              ThreadSafeRateLimiter* rateLimiter, ErrorInfo *error);

/* File Operations */
bool mergeFiles(Job **completedJobs, int jobCount, const char *outputDir);
bool mergeCSVFiles(const char *sourceFile, const char *targetFile, bool isFirst);
bool mergeJSONFiles(const char *sourceFile, const char *targetFile, bool isFirst);
void cleanupTempFiles(Job **completedJobs, int jobCount);

/* Utility Functions */
char* getCurrentDateTime(void);
char* formatDate(const char* dateString);
bool validateDateFormat(const char *date);
void buildRequestURL(char *url, size_t urlSize, const Endpoint *endpoint, 
                    const char *lastId, int skip, const char *beginDate, 
                    const char *endDate, ErrorInfo *error);
void updateJSONBracketDepth(const char *text, JSONMergeContext *ctx);
void writeJSONHeader(FILE *file, const char *key);
void writeJSONFooter(FILE *file);
bool processJSONItems(json_object *items, ProcessingContext *ctx);
bool processCSVItems(json_object *items, ProcessingContext *ctx);
void cleanupEndpointGroups(EndpointGroup *groups, int groupCount);
void parseCommandLine(int argc, char *argv[]);

Endpoint endpoints[] = {
    {
        .name = "pricelists",
        .key = "PriceLists",
        .pagination_type = NONE,
        .url_format = "https://api.repsly.com/v3/export/pricelists",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = true,
        .max_page_size = 100,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "clients",
        .key = "Clients",
        .pagination_type = TIMESTAMP,
        .url_format = "https://api.repsly.com/v3/export/clients/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = true,
        .include_inactive = true,
        .include_deleted = true,
        .max_page_size = 100,
        .required_parameters = "includeInactive=true",
        .default_date_range_days = 360,
        .requires_auth = true
    },
    {
        .name = "clientnotes",
        .key = "ClientNotes",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/clientnotes/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "representatives",
        .key = "Representatives",
        .pagination_type = NONE,
        .url_format = "https://api.repsly.com/v3/export/representatives",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = false,
        .max_page_size = 100,
        .required_parameters = "includeInactive=true",
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "forms",
        .key = "Forms",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/forms/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "users",
        .key = "Users",
        .pagination_type = TIMESTAMP,
        .url_format = "https://api.repsly.com/v3/export/users/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = true,
        .include_inactive = true,
        .include_deleted = false,
        .max_page_size = 100,
        .required_parameters = "includeInactive=true",
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "visits",
        .key = "Visits",
        .pagination_type = TIMESTAMP,
        .url_format = "https://api.repsly.com/v3/export/visits/%s",
        .use_raw_timestamp = true,
        .use_timestamp_pagination = true,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 7,  // Smaller default range due to volume
        .requires_auth = true
    },
    {
        .name = "visitrealizations",
        .key = "VisitRealizations",
        .pagination_type = SKIP,
        .url_format = "https://api.repsly.com/v3/export/visitrealizations?modified=%s&skip=%d",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 7,  // Smaller default range due to volume
        .requires_auth = true
    },
    {
        .name = "visitschedules",
        .key = "VisitSchedules",
        .pagination_type = DATE_RANGE,
        .url_format = "https://api.repsly.com/v3/export/visitschedules/%s/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 100,
        .required_parameters = NULL,
        .default_date_range_days = 7,  // Using weekly chunks for better handling
        .requires_auth = true
    },
    {
        .name = "dailyworkingtime",
        .key = "DailyWorkingTime",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/dailyworkingtime/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "products",
        .key = "Products",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/products/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = true,
        .max_page_size = 100,
        .required_parameters = "includeInactive=true",
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "photos",
        .key = "Photos",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/photos/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "documentTypes",
        .key = "DocumentTypes",
        .pagination_type = NONE,
        .url_format = "https://api.repsly.com/v3/export/documentTypes",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = true,
        .include_deleted = false,
        .max_page_size = 100,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "purchaseorders",
        .key = "PurchaseOrders",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/purchaseorders/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "retailaudits",
        .key = "RetailAudits",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/retailaudits/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    },
    {
        .name = "importstatus",
        .key = "ImportStatus",
        .pagination_type = ID,
        .url_format = "https://api.repsly.com/v3/export/importStatus/%s",
        .use_raw_timestamp = false,
        .use_timestamp_pagination = false,
        .include_inactive = false,
        .include_deleted = false,
        .max_page_size = 50,
        .required_parameters = NULL,
        .default_date_range_days = 30,
        .requires_auth = true
    }
};

static int num_endpoints = sizeof(endpoints) / sizeof(endpoints[0]);


/* Core Application Functions */
void initializeApplication(void) {
    // Initialize configuration
    config.rate_limit = DEFAULT_RATE_LIMIT_SECONDS;
    config.page_size = DEFAULT_PAGE_SIZE;
    config.retry_attempts = DEFAULT_RETRY_ATTEMPTS;
    config.timeout = DEFAULT_TIMEOUT;
    config.use_cache = 1;
    config.export_format = "csv";
    config.max_iterations = DEFAULT_MAX_ITERATIONS;
    
    // Initialize CURL
    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle = curl_easy_init();
    if (!curl_handle) {
        handleCriticalError("Failed to initialize CURL", NULL);
    }
    
    // Initialize logging
    if (config.log_file) {
        log_file_ptr = fopen(config.log_file, "a");
        if (!log_file_ptr) {
            fprintf(stderr, "Warning: Could not open log file %s: %s\n", 
                    config.log_file, strerror(errno));
        }
    }

    // Initialize mutexes
    pthread_mutex_init(&log_mutex, NULL);
    pthread_mutex_init(&curl_mutex, NULL);
}

void cleanupApplication(void) {
    // Cleanup CURL
    if (curl_handle) {
        curl_easy_cleanup(curl_handle);
        curl_handle = NULL;
    }
    curl_global_cleanup();
    
    // Cleanup logging
    if (log_file_ptr) {
        fclose(log_file_ptr);
        log_file_ptr = NULL;
    }
    
    // Cleanup mutexes
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&curl_mutex);
    
    // Cleanup configuration
    free(config.specific_endpoint);
    free(config.output_directory);
    free(config.from_date);
    free(config.to_date);
    if (config.export_format && strcmp(config.export_format, "csv") != 0) {
        free(config.export_format);
    }
    free(config.log_file);
}

void handleCriticalError(const char *message, ThreadPool *pool) {
    logMessage(LOG_ERROR, "CRITICAL ERROR: %s", message);
    
    if (pool) {
        cleanupThreadPool(pool);
    }
    cleanupApplication();
    exit(1);
}

void handleSignal(int signum) {
    char *signal_name = (signum == SIGINT) ? "SIGINT" : "SIGTERM";
    logMessage(LOG_INFO, "Received %s signal, initiating graceful shutdown...", signal_name);
    shutdown_requested = 1;
}




/* Thread Pool Management */
int initializeThreadPool(ThreadPool* pool, int numThreads) {
    if (!pool || numThreads <= 0) return -1;
    if (numThreads > MAX_THREADS) numThreads = MAX_THREADS;
    
    // Initialize pool state
    pool->shutdown = false;
    pool->active_threads = numThreads;
    
    // Initialize mutexes and job queue
    if (pthread_mutex_init(&pool->thread_count_mutex, NULL) != 0) {
        return -1;
    }
    
    if (initializeJobQueue(&pool->job_queue) != 0) {
        pthread_mutex_destroy(&pool->thread_count_mutex);
        return -1;
    }
    
    // Initialize rate limiter
    pool->rate_limiter = malloc(sizeof(ThreadSafeRateLimiter));
    if (!pool->rate_limiter) {
        cleanupJobQueue(&pool->job_queue);
        pthread_mutex_destroy(&pool->thread_count_mutex);
        return -1;
    }
    initializeRateLimiter(pool->rate_limiter);
    
    // Create worker threads
    for (int i = 0; i < numThreads; i++) {
        if (pthread_create(&pool->threads[i], NULL, workerThreadFunction, pool) != 0) {
            pool->shutdown = true;
            // Wait for already created threads
            for (int j = 0; j < i; j++) {
                pthread_join(pool->threads[j], NULL);
            }
            cleanupJobQueue(&pool->job_queue);
            free(pool->rate_limiter);
            pthread_mutex_destroy(&pool->thread_count_mutex);
            return -1;
        }
    }
    
    return 0;
}

void cleanupThreadPool(ThreadPool* pool) {
    if (!pool) return;
    
    // Signal shutdown
    pool->shutdown = true;
    
    // Wake up all waiting threads
    pthread_mutex_lock(&pool->job_queue.mutex);
    pthread_cond_broadcast(&pool->job_queue.not_empty);
    pthread_cond_broadcast(&pool->job_queue.not_full);
    pthread_mutex_unlock(&pool->job_queue.mutex);
    
    // Wait for all threads to finish
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    
    // Cleanup remaining jobs
    cleanupRemainingJobs(&pool->job_queue);
    
    // Cleanup synchronization primitives and resources
    pthread_mutex_destroy(&pool->thread_count_mutex);
    cleanupJobQueue(&pool->job_queue);
    
    if (pool->rate_limiter) {
        cleanupRateLimiter(pool->rate_limiter);
        free(pool->rate_limiter);
    }
}

void* workerThreadFunction(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;
    
    while (!pool->shutdown) {
        // Get next job
        Job* job = dequeueJob(&pool->job_queue);
        if (!job) continue;
        
        // Process the job
        processEndpoint(job->endpoint, job->temp_filename, 
                       pool->rate_limiter, &job->error);
        
        // Update job and thread status
        job->completed = true;
        
        pthread_mutex_lock(&pool->thread_count_mutex);
        pool->active_threads--;
        pthread_mutex_unlock(&pool->thread_count_mutex);
        
        // Log completion
        logMessage(LOG_INFO, "Completed processing endpoint: %s", 
                  job->endpoint->name);
    }
    
    return NULL;
}

void* monitorThreadFunction(void* arg) {
    MonitorData* data = (MonitorData*)arg;
    ThreadPool* pool = data->pool;
    
    while (data->active && !shutdown_requested) {
        pthread_mutex_lock(&pool->thread_count_mutex);
        
        logMessage(LOG_DEBUG, 
                  "Thread Pool Status - Active threads: %d, Queue size: %d, "
                  "Rate limit status: %d/%d requests",
                  pool->active_threads, 
                  pool->job_queue.size,
                  pool->rate_limiter->requests_in_window,
                  pool->rate_limiter->max_requests);
        
        pthread_mutex_unlock(&pool->thread_count_mutex);
        
        sleep(5);  // Monitor every 5 seconds
    }
    
    return NULL;
}



/* Job Management */
Job* createJob(Endpoint* endpoint, const char* outputDir) {
    if (!endpoint || !outputDir) return NULL;
    
    Job* job = (Job*)malloc(sizeof(Job));
    if (!job) {
        logMessage(LOG_ERROR, "Failed to allocate memory for job");
        return NULL;
    }
    
    // Initialize basic job properties
    job->endpoint = endpoint;
    job->completed = false;
    job->output_directory = strdup(outputDir);
    memset(&job->error, 0, sizeof(ErrorInfo));
    
    // Create unique temporary filename
    char temp_filename[MAX_URL_LENGTH];
    snprintf(temp_filename, sizeof(temp_filename), "%s/Repsly_%s_Export_%ld.tmp.%s",
             outputDir, endpoint->key, (long)time(NULL), config.export_format);
    job->temp_filename = strdup(temp_filename);
    
    if (!job->output_directory || !job->temp_filename) {
        logMessage(LOG_ERROR, "Failed to allocate strings for job");
        destroyJob(job);
        return NULL;
    }
    
    return job;
}

void destroyJob(Job* job) {
    if (!job) return;
    
    free(job->output_directory);
    free(job->temp_filename);
    free(job);
}

int initializeJobQueue(JobQueue* queue) {
    if (!queue) return -1;
    
    queue->front = 0;
    queue->rear = -1;
    queue->size = 0;
    
    if (pthread_mutex_init(&queue->mutex, NULL) != 0) {
        return -1;
    }
    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        return -1;
    }
    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_mutex_destroy(&queue->mutex);
        pthread_cond_destroy(&queue->not_empty);
        return -1;
    }
    
    return 0;
}

void cleanupJobQueue(JobQueue* queue) {
    if (!queue) return;
    
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
}

bool enqueueJob(JobQueue* queue, Job* job) {
    if (!queue || !job) return false;
    
    pthread_mutex_lock(&queue->mutex);
    
    while (queue->size >= MAX_QUEUE_SIZE && !shutdown_requested) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    
    if (shutdown_requested) {
        pthread_mutex_unlock(&queue->mutex);
        return false;
    }
    
    queue->rear = (queue->rear + 1) % MAX_QUEUE_SIZE;
    queue->jobs[queue->rear] = job;
    queue->size++;
    
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
    
    logMessage(LOG_DEBUG, "Enqueued job for endpoint: %s", job->endpoint->name);
    return true;
}

Job* dequeueJob(JobQueue* queue) {
    if (!queue) return NULL;
    
    pthread_mutex_lock(&queue->mutex);
    
    while (queue->size == 0 && !shutdown_requested) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    
    if (shutdown_requested && queue->size == 0) {
        pthread_mutex_unlock(&queue->mutex);
        return NULL;
    }
    
    Job* job = queue->jobs[queue->front];
    queue->front = (queue->front + 1) % MAX_QUEUE_SIZE;
    queue->size--;
    
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
    
    if (job) {
        logMessage(LOG_DEBUG, "Dequeued job for endpoint: %s", job->endpoint->name);
    }
    return job;
}

void cleanupRemainingJobs(JobQueue* queue) {
    if (!queue) return;
    
    pthread_mutex_lock(&queue->mutex);
    while (queue->size > 0) {
        Job* job = queue->jobs[queue->front];
        if (job) {
            destroyJob(job);
        }
        queue->front = (queue->front + 1) % MAX_QUEUE_SIZE;
        queue->size--;
    }
    pthread_mutex_unlock(&queue->mutex);
}




/* Rate Limiting */
void initializeRateLimiter(ThreadSafeRateLimiter* limiter) {
    if (!limiter) return;
    
    if (pthread_mutex_init(&limiter->mutex, NULL) != 0 ||
        pthread_cond_init(&limiter->rate_limit_cv, NULL) != 0) {
        handleCriticalError("Failed to initialize rate limiter synchronization", NULL);
    }
    
    limiter->last_request = 0;
    limiter->requests_in_window = 0;
    limiter->window_size = DEFAULT_WINDOW_SIZE;
    limiter->max_requests = DEFAULT_MAX_REQUESTS;
    limiter->min_interval = MIN_REQUEST_INTERVAL;
    limiter->backoff_active = false;
    limiter->backoff_multiplier = 1;
    limiter->avg_response_time = 0;
    limiter->active_requests = 0;
    limiter->total_requests = 0;
}

void cleanupRateLimiter(ThreadSafeRateLimiter* limiter) {
    if (!limiter) return;
    
    pthread_mutex_destroy(&limiter->mutex);
    pthread_cond_destroy(&limiter->rate_limit_cv);
}

void applyRateLimit(ThreadSafeRateLimiter* limiter, ErrorInfo* error) {
    if (!limiter) return;
    
    struct timespec wait_time;
    pthread_mutex_lock(&limiter->mutex);
    
    time_t now = time(NULL);
    double time_since_last = difftime(now, limiter->last_request);
    
    // Handle minimum interval
    if (time_since_last < limiter->min_interval) {
        clock_gettime(CLOCK_REALTIME, &wait_time);
        wait_time.tv_sec += (time_t)ceil(limiter->min_interval - time_since_last);
        pthread_cond_timedwait(&limiter->rate_limit_cv, &limiter->mutex, &wait_time);
    }
    
    // Reset window if needed
    if (difftime(now, limiter->last_request) >= limiter->window_size) {
        limiter->requests_in_window = 0;
        limiter->last_request = now;
    }
    
    // Apply exponential backoff if active
    if (limiter->backoff_active) {
        clock_gettime(CLOCK_REALTIME, &wait_time);
        wait_time.tv_sec += (time_t)(limiter->min_interval * limiter->backoff_multiplier);
        pthread_cond_timedwait(&limiter->rate_limit_cv, &limiter->mutex, &wait_time);
    }
    
    // Wait if at rate limit
    while (limiter->requests_in_window >= limiter->max_requests) {
        clock_gettime(CLOCK_REALTIME, &wait_time);
        wait_time.tv_sec = limiter->last_request + limiter->window_size;
        pthread_cond_timedwait(&limiter->rate_limit_cv, &limiter->mutex, &wait_time);
        
        now = time(NULL);
        if (difftime(now, limiter->last_request) >= limiter->window_size) {
            limiter->requests_in_window = 0;
            limiter->last_request = now;
        }
    }
    
    // Update state
    limiter->requests_in_window++;
    limiter->active_requests++;
    limiter->last_request = now;
    limiter->total_requests++;
    
    pthread_mutex_unlock(&limiter->mutex);
}

void handleRateLimitResponse(ThreadSafeRateLimiter* limiter, int httpStatus, int responseTime) {
    if (!limiter) return;
    
    pthread_mutex_lock(&limiter->mutex);
    
    limiter->active_requests--;
    
    // Update moving average of response time
    limiter->avg_response_time = (0.1 * responseTime) + 
                                (0.9 * limiter->avg_response_time);
    
    // Handle response status
    switch (httpStatus) {
        case 429: // Too Many Requests
            limiter->backoff_active = true;
            limiter->backoff_multiplier *= 2;
            if (limiter->backoff_multiplier > 32) {
                limiter->backoff_multiplier = 32;
            }
            logMessage(LOG_WARNING, "Rate limit exceeded, backing off (multiplier: %d)", 
                      limiter->backoff_multiplier);
            break;
            
        case 200: // Success
            if (limiter->backoff_active) {
                limiter->backoff_multiplier = limiter->backoff_multiplier > 1 ? 
                                            limiter->backoff_multiplier / 2 : 1;
                if (limiter->backoff_multiplier == 1) {
                    limiter->backoff_active = false;
                    logMessage(LOG_INFO, "Rate limit backoff deactivated");
                }
            }
            break;
            
        default:
            if (!limiter->backoff_active) {
                limiter->backoff_active = true;
                limiter->backoff_multiplier = 1;
                logMessage(LOG_WARNING, "Unexpected response, activating rate limit backoff");
            }
            break;
    }
    
    // Adjust rate limits based on performance
    adjustRateLimits(limiter);
    
    pthread_cond_broadcast(&limiter->rate_limit_cv);
    pthread_mutex_unlock(&limiter->mutex);
}

void adjustRateLimits(ThreadSafeRateLimiter* limiter) {
    if (!limiter) return;
    
    if (limiter->avg_response_time > 2000) {
        limiter->max_requests = (int)(limiter->max_requests * 0.8);
        if (limiter->max_requests < 10) limiter->max_requests = 10;
        logMessage(LOG_DEBUG, "Reducing rate limit to %d requests per window", 
                  limiter->max_requests);
    } else if (limiter->avg_response_time < 500 && 
               limiter->max_requests < DEFAULT_MAX_REQUESTS) {
        limiter->max_requests = (int)(limiter->max_requests * 1.1);
        if (limiter->max_requests > DEFAULT_MAX_REQUESTS) {
            limiter->max_requests = DEFAULT_MAX_REQUESTS;
        }
        logMessage(LOG_DEBUG, "Increasing rate limit to %d requests per window", 
                  limiter->max_requests);
    }
}



/* Data Processing */
int processEndpoint(const Endpoint *endpoint, const char *tempFilename, 
                   ThreadSafeRateLimiter *rateLimiter, ErrorInfo *error) {
    if (!endpoint || !tempFilename || !rateLimiter || !error) {
        setError(error, "Invalid parameters for endpoint processing", -1);
        return -1;
    }

    ProcessingContext ctx = {
        .url = {0},
        .chunk = {0},
        .last_id = "0",
        .pagination = {0},
        .skip = 0,
        .begin_date = NULL,
        .end_date = NULL,
        .output_file = NULL,
        .csv_state = {0},
        .first_batch = true,
        .result = -1
    };

    // Initialize dates
    if (!initializeDateRange(endpoint, &ctx.begin_date, &ctx.end_date)) {
        setError(error, "Failed to initialize date range", -1);
        goto cleanup;
    }

    // Initialize memory chunk
    ctx.chunk.memory = malloc(1);
    if (!ctx.chunk.memory) {
        setError(error, "Failed to allocate initial memory", -1);
        goto cleanup;
    }
    ctx.chunk.size = 0;

    // Initialize CURL
    CURL *thread_curl = curl_easy_init();
    if (!thread_curl) {
        setError(error, "Failed to initialize CURL", -1);
        goto cleanup;
    }

    // Open output file
    ctx.output_file = openOutputFile(tempFilename, error);
    if (!ctx.output_file) goto cleanup;

    // Write JSON header if needed
    if (isJSONFormat()) {
        writeJSONHeader(ctx.output_file, endpoint->key);
    }

    // Initialize pagination
    ctx.pagination.page_number = 1;
    ctx.pagination.has_more = true;
    ctx.pagination.records_processed = 0;

    // Main processing loop
    while (shouldContinueProcessing(&ctx.pagination) && !shutdown_requested) {
        // Build request URL
        if (!buildRequestURL(ctx.url, sizeof(ctx.url), endpoint, ctx.last_id, 
                           ctx.skip, ctx.begin_date, ctx.end_date, error)) {
            goto cleanup;
        }

        // Log request if verbose
        logRequestDetails(endpoint, &ctx.pagination, ctx.url);

        // Fetch and process data
        if (!fetchAndProcessData(endpoint, &ctx, thread_curl, rateLimiter, error)) {
            goto cleanup;
        }

        // Update pagination state
        if (!updatePaginationState(endpoint, &ctx)) {
            ctx.pagination.has_more = false;
        }
    }

    // Write JSON footer if needed
    if (isJSONFormat()) {
        writeJSONFooter(ctx.output_file);
    }

    ctx.result = 0;

cleanup:
    cleanupProcessingContext(&ctx, thread_curl);
    return ctx.result;
}

bool fetchAndProcessData(const Endpoint *endpoint, ProcessingContext *ctx, 
                        CURL *thread_curl, ThreadSafeRateLimiter *rateLimiter, 
                        ErrorInfo *error) {
    bool success = false;
    
    // Attempt fetch with retries
    for (int retry = 0; retry < config.retry_attempts && !shutdown_requested; retry++) {
        if (fetchData(ctx->url, &ctx->chunk, thread_curl, rateLimiter, error) == 0) {
            success = true;
            break;
        }
        
        if (shouldAbortRetry(error)) break;
        handleFetchError(error, retry);
    }

    if (!success) {
        logMessage(LOG_ERROR, "Failed to fetch data after %d attempts", 
                  config.retry_attempts);
        return false;
    }

    // Parse and validate response
    json_object *parsed_json = parseAndValidateResponse(ctx->chunk.memory, endpoint, error);
    if (!parsed_json) return false;

    // Process response data
    success = processResponseData(endpoint, parsed_json, ctx);
    
    json_object_put(parsed_json);
    return success;
}

int fetchData(const char* url, MemoryStruct* chunk, CURL *thread_curl,
              ThreadSafeRateLimiter* rateLimiter, ErrorInfo *error) {
    // Apply rate limiting
    applyRateLimit(rateLimiter, error);
    if (error->code != 0) return -1;

    CURLcode res;
    struct curl_slist *headers = NULL;
    long response_code = 0;
    double response_time = 0;

    pthread_mutex_lock(&curl_mutex);
    
    // Setup request
    if (!setupCURLRequest(thread_curl, url, chunk, &headers, error)) {
        pthread_mutex_unlock(&curl_mutex);
        return -1;
    }

    // Perform request
    res = curl_easy_perform(thread_curl);
    
    if (res != CURLE_OK) {
        handleCURLError(res, error);
        curl_slist_free_all(headers);
        pthread_mutex_unlock(&curl_mutex);
        return -1;
    }

    // Get response info
    curl_easy_getinfo(thread_curl, CURLINFO_RESPONSE_CODE, &response_code);
    curl_easy_getinfo(thread_curl, CURLINFO_TOTAL_TIME, &response_time);

    curl_slist_free_all(headers);
    pthread_mutex_unlock(&curl_mutex);

    // Handle rate limiting
    handleRateLimitResponse(rateLimiter, response_code, (int)(response_time * 1000));

    // Check response code
    if (response_code != 200) {
        setError(error, "HTTP error", response_code);
        return -1;
    }

    return 0;
}



/* File Operations */
bool mergeFiles(Job **completedJobs, int jobCount, const char *outputDir) {
    if (jobCount == 0 || !outputDir) return true;

    EndpointGroup *groups = createEndpointGroups(completedJobs, jobCount);
    if (!groups) {
        logMessage(LOG_ERROR, "Failed to create endpoint groups");
        return false;
    }

    int groupCount = populateEndpointGroups(groups, completedJobs, jobCount);
    bool success = processEndpointGroups(groups, groupCount, outputDir);

    // Cleanup
    cleanupEndpointGroups(groups, groupCount);
    
    if (success) {
        cleanupTempFiles(completedJobs, jobCount);
    }

    return success;
}

EndpointGroup* createEndpointGroups(Job **jobs, int jobCount) {
    EndpointGroup *groups = calloc(num_endpoints, sizeof(EndpointGroup));
    if (!groups) return NULL;

    // Initialize groups
    for (int i = 0; i < num_endpoints; i++) {
        groups[i].endpoint = NULL;
        groups[i].jobs = NULL;
        groups[i].count = 0;
    }

    return groups;
}

int populateEndpointGroups(EndpointGroup *groups, Job **jobs, int jobCount) {
    int groupCount = 0;

    // Count jobs per endpoint
    for (int i = 0; i < jobCount; i++) {
        if (!jobs[i]) continue;

        bool found = false;
        for (int j = 0; j < groupCount; j++) {
            if (groups[j].endpoint == jobs[i]->endpoint) {
                groups[j].count++;
                found = true;
                break;
            }
        }

        if (!found) {
            groups[groupCount].endpoint = jobs[i]->endpoint;
            groups[groupCount].count = 1;
            groupCount++;
        }
    }

    // Allocate job arrays
    for (int i = 0; i < groupCount; i++) {
        groups[i].jobs = calloc(groups[i].count, sizeof(Job*));
        if (!groups[i].jobs) {
            return -1;
        }
    }

    // Fill job arrays
    int *indices = calloc(groupCount, sizeof(int));
    for (int i = 0; i < jobCount; i++) {
        if (!jobs[i]) continue;

        for (int j = 0; j < groupCount; j++) {
            if (groups[j].endpoint == jobs[i]->endpoint) {
                groups[j].jobs[indices[j]++] = jobs[i];
                break;
            }
        }
    }

    free(indices);
    return groupCount;
}

bool processEndpointGroups(EndpointGroup *groups, int groupCount, const char *outputDir) {
    bool success = true;

    for (int i = 0; i < groupCount && !shutdown_requested; i++) {
        char finalFilename[MAX_URL_LENGTH];
        snprintf(finalFilename, sizeof(finalFilename), "%s/Repsly_%s_Export.%s",
                outputDir, groups[i].endpoint->key, config.export_format);

        if (!mergeGroupFiles(&groups[i], finalFilename)) {
            logMessage(LOG_ERROR, "Failed to merge files for endpoint: %s", 
                      groups[i].endpoint->name);
            success = false;
            break;
        }
    }

    return success;
}

bool mergeGroupFiles(EndpointGroup *group, const char *finalFilename) {
    for (int i = 0; i < group->count; i++) {
        bool isFirst = (i == 0);
        bool mergeSuccess;

        if (isJSONFormat()) {
            mergeSuccess = mergeJSONFiles(group->jobs[i]->temp_filename,
                                        finalFilename, isFirst);
        } else {
            mergeSuccess = mergeCSVFiles(group->jobs[i]->temp_filename,
                                       finalFilename, isFirst);
        }

        if (!mergeSuccess) return false;
    }

    return validateMergedFile(finalFilename);
}

bool mergeCSVFiles(const char *sourceFile, const char *targetFile, bool isFirst) {
    FILE *source = fopen(sourceFile, "r");
    if (!source) {
        logMessage(LOG_ERROR, "Failed to open source file: %s", sourceFile);
        return false;
    }

    FILE *target = fopen(targetFile, isFirst ? "w" : "a");
    if (!target) {
        fclose(source);
        logMessage(LOG_ERROR, "Failed to open target file: %s", targetFile);
        return false;
    }

    char buffer[MAX_BUFFER];
    bool skipHeader = !isFirst;
    bool firstLine = true;

    while (fgets(buffer, sizeof(buffer), source) && !shutdown_requested) {
        if (skipHeader && firstLine) {
            firstLine = false;
            continue;
        }
        fputs(buffer, target);
    }

    fclose(source);
    fclose(target);
    return true;
}

bool mergeJSONFiles(const char *sourceFile, const char *targetFile, bool isFirst) {
    FILE *source = fopen(sourceFile, "r");
    if (!source) {
        logMessage(LOG_ERROR, "Failed to open source file: %s", sourceFile);
        return false;
    }

    FILE *target = fopen(targetFile, isFirst ? "w" : "a");
    if (!target) {
        fclose(source);
        logMessage(LOG_ERROR, "Failed to open target file: %s", targetFile);
        return false;
    }

    JSONMergeContext ctx = {
        .inArray = false,
        .skipBrackets = !isFirst,
        .bracketDepth = 0
    };

    if (!mergeJSONContent(source, target, &ctx)) {
        fclose(source);
        fclose(target);
        return false;
    }

    fclose(source);
    fclose(target);
    return true;
}

bool mergeJSONContent(FILE *source, FILE *target, JSONMergeContext *ctx) {
    char buffer[MAX_BUFFER];

    while (fgets(buffer, sizeof(buffer), source) && !shutdown_requested) {
        char *pos = buffer;
        
        if (ctx->skipBrackets) {
            char *bracketPos = strchr(buffer, '[');
            if (bracketPos) {
                pos = bracketPos + 1;
                ctx->skipBrackets = false;
            } else {
                continue;
            }
        }

        if (!ctx->inArray && !ctx->skipBrackets) {
            if (strchr(pos, '[')) {
                ctx->inArray = true;
                fprintf(target, ",\n");
            }
        }

        updateJSONBracketDepth(pos, ctx);
        fputs(pos, target);
    }

    return true;
}

void cleanupTempFiles(Job **completedJobs, int jobCount) {
    for (int i = 0; i < jobCount; i++) {
        if (completedJobs[i] && completedJobs[i]->temp_filename) {
            if (remove(completedJobs[i]->temp_filename) != 0) {
                logMessage(LOG_WARNING, "Failed to remove temporary file: %s", 
                          completedJobs[i]->temp_filename);
            }
        }
    }
}

bool validateMergedFile(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) return false;

    bool isValid = true;
    char buffer[MAX_BUFFER];

    if (isJSONFormat()) {
        isValid = validateJSONStructure(file);
    } else {
        isValid = validateCSVStructure(file);
    }

    fclose(file);
    return isValid;
}

bool validateJSONStructure(FILE *file) {
    char buffer[MAX_BUFFER];
    int braceCount = 0;
    int bracketCount = 0;
    
    while (fgets(buffer, sizeof(buffer), file) && !shutdown_requested) {
        for (char *p = buffer; *p; p++) {
            switch (*p) {
                case '{': braceCount++; break;
                case '}': braceCount--; break;
                case '[': bracketCount++; break;
                case ']': bracketCount--; break;
            }
            
            if (braceCount < 0 || bracketCount < 0) return false;
        }
    }
    
    return (braceCount == 0 && bracketCount == 0);
}

bool validateCSVStructure(FILE *file) {
    char buffer[MAX_BUFFER];
    int expectedFields = -1;
    int lineNumber = 0;
    
    while (fgets(buffer, sizeof(buffer), file) && 
           lineNumber < 10 && !shutdown_requested) {
        int currentFields = countCSVFields(buffer);
        
        if (expectedFields == -1) {
            expectedFields = currentFields;
        } else if (currentFields != expectedFields) {
            return false;
        }
        
        lineNumber++;
    }
    
    return (lineNumber > 0);
}

int countCSVFields(const char *line) {
    int fields = 1;
    bool inQuotes = false;
    
    for (const char *p = line; *p; p++) {
        if (*p == '"') {
            inQuotes = !inQuotes;
        } else if (*p == ',' && !inQuotes) {
            fields++;
        }
    }
    
    return fields;
}



/* Utility Functions */
void logMessage(LogLevel level, const char *format, ...) {
    if (!format) return;
    
    pthread_mutex_lock(&log_mutex);
    
    time_t now = time(NULL);
    char timestamp[26];
    struct tm tm_info;
    localtime_r(&now, &tm_info);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm_info);
    
    const char *level_str = "";
    switch (level) {
        case LOG_DEBUG:   level_str = "DEBUG"; break;
        case LOG_INFO:    level_str = "INFO"; break;
        case LOG_WARNING: level_str = "WARNING"; break;
        case LOG_ERROR:   level_str = "ERROR"; break;
    }
    
    va_list args;
    va_start(args, format);
    
    if (log_file_ptr) {
        fprintf(log_file_ptr, "[%s] [%s] ", timestamp, level_str);
        vfprintf(log_file_ptr, format, args);
        fprintf(log_file_ptr, "\n");
        fflush(log_file_ptr);
    }
    
    if ((config.verbose_mode && level >= LOG_INFO) || 
        (config.debug_mode && level >= LOG_DEBUG)) {
        fprintf(stderr, "[%s] [%s] ", timestamp, level_str);
        vfprintf(stderr, format, args);
        fprintf(stderr, "\n");
    }
    
    va_end(args);
    pthread_mutex_unlock(&log_mutex);
}

char* getCurrentDateTime(void) {
    char* datetime = malloc(MAX_DATE_LENGTH);
    if (!datetime) return NULL;
    
    time_t now = time(NULL);
    struct tm tm_info;
    gmtime_r(&now, &tm_info);
    
    strftime(datetime, MAX_DATE_LENGTH - 1, "%Y-%m-%d", &tm_info);
    return datetime;
}

bool initializeDateRange(const Endpoint *endpoint, char **beginDate, char **endDate) {
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    
    // Initialize end date
    *endDate = config.to_date ? strdup(config.to_date) : getCurrentDateTime();
    if (!*endDate) return false;
    
    // Initialize begin date
    if (config.from_date) {
        *beginDate = strdup(config.from_date);
    } else {
        time_t start = now - (endpoint->default_date_range_days * 24 * 60 * 60);
        struct tm tm_start;
        localtime_r(&start, &tm_start);
        
        *beginDate = malloc(MAX_DATE_LENGTH);
        if (*beginDate) {
            strftime(*beginDate, MAX_DATE_LENGTH, "%Y-%m-%d", &tm_start);
        }
    }
    
    if (!*beginDate) {
        free(*endDate);
        return false;
    }
    
    return true;
}

void setError(ErrorInfo *error, const char *message, int code) {
    if (!error) return;
    
    snprintf(error->message, MAX_ERROR_LENGTH, "%s", message);
    error->code = code;
}

bool shouldContinueProcessing(const PaginationState *pagination) {
    return pagination->has_more && 
           pagination->page_number < config.max_iterations && 
           !shutdown_requested;
}

bool shouldAbortRetry(const ErrorInfo *error) {
    return error->code >= 400 && error->code < 500;  // Client errors
}

void handleFetchError(ErrorInfo *error, int retryCount) {
    logMessage(LOG_WARNING, "Fetch attempt %d failed: %s (code: %d)", 
               retryCount + 1, error->message, error->code);
    
    // Exponential backoff for retries
    sleep((1 << retryCount) * config.rate_limit);
}

json_object* parseAndValidateResponse(const char *response, 
                                    const Endpoint *endpoint, 
                                    ErrorInfo *error) {
    json_object *parsed_json = json_tokener_parse(response);
    if (!parsed_json) {
        setError(error, "Failed to parse JSON response", -1);
        return NULL;
    }
    
    if (!validateResponseFormat(parsed_json, endpoint)) {
        setError(error, "Invalid response format", -1);
        json_object_put(parsed_json);
        return NULL;
    }
    
    return parsed_json;
}

bool processResponseData(const Endpoint *endpoint, json_object *parsed_json, 
                        ProcessingContext *ctx) {
    json_object *items;
    if (!json_object_object_get_ex(parsed_json, endpoint->key, &items)) {
        return false;
    }
    
    int n_items = json_object_array_length(items);
    if (n_items == 0) {
        ctx->pagination.has_more = false;
        return true;
    }
    
    return isJSONFormat() ? 
           processJSONItems(items, ctx) : 
           processCSVItems(items, ctx);
}

bool setupCURLRequest(CURL *curl, const char *url, MemoryStruct *chunk, 
                     struct curl_slist **headers, ErrorInfo *error) {
    const char* api_username = getenv("REPSLY_API_USERNAME");
    const char* api_password = getenv("REPSLY_API_PASSWORD");
    
    if (!api_username || !api_password) {
        setError(error, "API credentials not set", -1);
        return false;
    }
    
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)chunk);
    curl_easy_setopt(curl, CURLOPT_USERNAME, api_username);
    curl_easy_setopt(curl, CURLOPT_PASSWORD, api_password);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, config.timeout);
    
    *headers = curl_slist_append(NULL, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);
    
    return true;
}

static size_t writeMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    MemoryStruct *mem = (MemoryStruct *)userp;
    
    char *ptr = realloc(mem->memory, mem->size + realsize + 1);
    if (!ptr) {
        logMessage(LOG_ERROR, "Failed to allocate memory (realloc returned NULL)");
        return 0;
    }
    
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    
    return realsize;
}

void cleanupProcessingContext(ProcessingContext *ctx, CURL *curl) {
    if (ctx->output_file) fclose(ctx->output_file);
    if (ctx->chunk.memory) free(ctx->chunk.memory);
    if (ctx->begin_date) free(ctx->begin_date);
    if (ctx->end_date) free(ctx->end_date);
    if (curl) curl_easy_cleanup(curl);
    cleanupCSVState(&ctx->csv_state);
}

bool isJSONFormat(void) {
    return strcmp(config.export_format, "json") == 0;
}

void handleCURLError(CURLcode res, ErrorInfo *error) {
    setError(error, curl_easy_strerror(res), res);
}

FILE* openOutputFile(const char *filename, ErrorInfo *error) {
    FILE *file = fopen(filename, isJSONFormat() ? "a" : "w");
    if (!file) {
        setError(error, "Failed to open output file", errno);
    }
    return file;
}


int main(int argc, char *argv[]) {
    int result = 0;
    MonitorData monitor_data = {0};
    pthread_t monitor_thread;
    
    // Setup signal handlers
    signal(SIGINT, handleSignal);
    signal(SIGTERM, handleSignal);
    
    // Parse command line and initialize
    parseCommandLine(argc, argv);
    initializeApplication();
    
    // Initialize thread pool
    ThreadPool pool;
    if (initializeThreadPool(&pool, MAX_THREADS) != 0) {
        handleCriticalError("Failed to initialize thread pool", NULL);
    }
    
    // Start monitoring thread if in debug mode
    if (config.debug_mode) {
        monitor_data.pool = &pool;
        monitor_data.active = true;
        if (pthread_create(&monitor_thread, NULL, monitorThreadFunction, &monitor_data) != 0) {
            logMessage(LOG_WARNING, "Failed to create monitoring thread");
        }
    }
    
    // Create job array
    int max_jobs = num_endpoints;
    Job **jobs = calloc(max_jobs, sizeof(Job*));
    if (!jobs) {
        handleCriticalError("Failed to allocate memory for jobs", &pool);
    }
    
    // Create and queue jobs
    int job_count = 0;
    for (int i = 0; i < num_endpoints && !shutdown_requested; i++) {
        // Skip if not the specified endpoint
        if (config.specific_endpoint && 
            strcmp(config.specific_endpoint, endpoints[i].name) != 0) {
            continue;
        }
        
        // Create job
        Job* job = createJob(&endpoints[i], 
                           config.output_directory ? config.output_directory : ".");
        if (!job) {
            logMessage(LOG_ERROR, "Failed to create job for endpoint: %s", 
                      endpoints[i].name);
            continue;
        }
        
        // Store and enqueue job
        jobs[job_count++] = job;
        if (!enqueueJob(&pool.job_queue, job)) {
            logMessage(LOG_ERROR, "Failed to enqueue job for endpoint: %s", 
                      endpoints[i].name);
            destroyJob(job);
            jobs[job_count - 1] = NULL;
            job_count--;
        }
    }
    
    // Process completion loop
    bool all_complete;
    do {
        if (shutdown_requested) {
            logMessage(LOG_INFO, "Shutdown requested, waiting for active jobs to complete...");
            break;
        }
        
        all_complete = true;
        sleep(1);
        
        pthread_mutex_lock(&pool.thread_count_mutex);
        for (int i = 0; i < job_count; i++) {
            if (jobs[i] && !jobs[i]->completed) {
                all_complete = false;
                break;
            }
        }
        pthread_mutex_unlock(&pool.thread_count_mutex);
        
    } while (!all_complete && pool.active_threads > 0);
    
    // Stop monitoring thread if active
    if (config.debug_mode) {
        monitor_data.active = false;
        pthread_join(monitor_thread, NULL);
    }
    
    // Check for failed jobs
    for (int i = 0; i < job_count; i++) {
        if (jobs[i] && jobs[i]->error.code != 0) {
            logMessage(LOG_ERROR, "Job failed for endpoint %s: %s", 
                      jobs[i]->endpoint->name, jobs[i]->error.message);
            result = 1;
        }
    }
    
    // Merge output files if successful and not shutdown
    if (result == 0 && !shutdown_requested) {
        if (!mergeFiles(jobs, job_count, 
                       config.output_directory ? config.output_directory : ".")) {
            logMessage(LOG_ERROR, "Failed to merge output files");
            result = 1;
        }
    }
    
    // Final statistics
    if (config.verbose_mode || config.debug_mode) {
        printFinalStats(jobs, job_count);
    }
    
    // Cleanup
    cleanupJobs(jobs, job_count, shutdown_requested);
    cleanupThreadPool(&pool);
    cleanupApplication();
    
    #ifdef DEBUG
    checkMemoryLeaks();
    #endif
    
    logMessage(LOG_INFO, "Application %s. Processed %d endpoints.", 
               result == 0 ? "completed successfully" : "completed with errors",
               job_count);
    
    return result;
}

void printFinalStats(Job **jobs, int job_count) {
    int completed = 0;
    int failed = 0;
    int records = 0;
    
    for (int i = 0; i < job_count; i++) {
        if (!jobs[i]) continue;
        
        if (jobs[i]->completed) {
            completed++;
            records += jobs[i]->records_processed;
        } else {
            failed++;
        }
    }
    
    logMessage(LOG_INFO, "\nFinal Statistics:");
    logMessage(LOG_INFO, "----------------");
    logMessage(LOG_INFO, "Total Jobs: %d", job_count);
    logMessage(LOG_INFO, "Completed: %d", completed);
    logMessage(LOG_INFO, "Failed: %d", failed);
    logMessage(LOG_INFO, "Total Records Processed: %d", records);
    logMessage(LOG_INFO, "----------------\n");
}

void cleanupJobs(Job **jobs, int job_count, bool was_shutdown) {
    for (int i = 0; i < job_count; i++) {
        if (jobs[i]) {
            if (was_shutdown && !jobs[i]->completed) {
                logMessage(LOG_WARNING, "Job for endpoint %s was interrupted", 
                          jobs[i]->endpoint->name);
            }
            destroyJob(jobs[i]);
        }
    }
    free(jobs);
}