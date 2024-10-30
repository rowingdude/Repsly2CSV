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

/* Endpoint Pagination */
typedef struct {
    int records_processed;
    int total_records;
    char last_value[64];
    int page_number;
    bool has_more;
    time_t last_timestamp;
} PaginationState;

/* CSV State */
typedef struct {
    char **headers;
    int header_count;
    char *last_row_values;
    size_t last_row_size;
} CSVState;

/* Endpoint Configuration */
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

/* Error Handling */
typedef struct {
    char message[MAX_ERROR_LENGTH];
    int code;
    const char *endpoint;
    const char *url;
} ErrorInfo;

/* Globals */
CURL *curl_handle;
AppConfig config = {0};
FILE *log_file_ptr = NULL;

/* Function Declarations */

void initialize_app(void);
void cleanup_app(void);
void initialize_curl(void);
void cleanup_curl(void);

void parse_command_line(int argc, char *argv[]);
void validate_config(void);
void print_help(void);

void log_message(const char *format, ...);
void log_error(ErrorInfo *error);
void handle_error(ErrorInfo *error);

static size_t write_memory_callback(void *contents, size_t size, size_t nmemb, void *userp);
int fetch_data(const char* url, MemoryStruct* chunk, ErrorInfo *error);
int fetch_data_with_backoff(const char* url, MemoryStruct* chunk, int attempt, ErrorInfo *error);

char* get_current_datetime(void);
char* convert_date(const char* date_string);
void split_date_range(const char *start_date, const char *end_date, char **current_start, char **current_end);

void construct_url(char *url, size_t url_size, const Endpoint *endpoint, 
                  const char *last_id, int skip, const char *begin_date, 
                  const char *end_date, ErrorInfo *error);
void add_query_parameters(char *url, size_t url_size, const Endpoint *endpoint);

bool validate_response_format(const json_object *parsed_json, const Endpoint *endpoint);
bool is_valid_response(const char *response, const Endpoint *endpoint);
bool update_pagination(const Endpoint *endpoint, json_object *parsed_json, 
                      char *last_id, size_t last_id_size, int *skip, 
                      char **begin_date, char **end_date);

void write_csv_header(FILE *csv_file, json_object *items, CSVState *csv_state);
void write_csv_row(FILE *csv_file, json_object *item, bool convert_timestamp, CSVState *csv_state);
void cleanup_csv_state(CSVState *csv_state);

bool load_cache(const char *cache_filename, char *last_id, size_t last_id_size);
void save_cache(const char *cache_filename, const char *last_id);

int process_endpoint(const Endpoint *endpoint);
bool is_duplicate_record(json_object *item, const char *last_id, bool use_timestamp);

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

void initialize_app(void) {
    // Set default configuration values
    config.rate_limit = DEFAULT_RATE_LIMIT_SECONDS;
    config.page_size = DEFAULT_PAGE_SIZE;
    config.retry_attempts = DEFAULT_RETRY_ATTEMPTS;
    config.timeout = DEFAULT_TIMEOUT;
    config.use_cache = 1;
    config.export_format = "csv";
    config.max_iterations = DEFAULT_MAX_ITERATIONS;
    
    // Initialize CURL
    initialize_curl();
    
    // Open log file if specified
    if (config.log_file) {
        log_file_ptr = fopen(config.log_file, "a");
        if (!log_file_ptr) {
            fprintf(stderr, "Warning: Could not open log file %s: %s\n", 
                    config.log_file, strerror(errno));
        }
    }
}

void cleanup_app(void) {
    cleanup_curl();
    if (log_file_ptr) {
        fclose(log_file_ptr);
    }
}

void initialize_curl(void) {
    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle = curl_easy_init();
    if (!curl_handle) {
        fprintf(stderr, "Error: Failed to initialize CURL\n");
        exit(1);
    }
}

void cleanup_curl(void) {
    if (curl_handle) {
        curl_easy_cleanup(curl_handle);
        curl_handle = NULL;
    }
    curl_global_cleanup();
}

void log_error(ErrorInfo *error) {
    if (!error) return;
    
    time_t now = time(NULL);
    char timestamp[26];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));
    
    if (log_file_ptr) {
        fprintf(log_file_ptr, "[%s] ERROR: %s\nEndpoint: %s\nURL: %s\nCode: %d\n\n",
                timestamp, error->message, error->endpoint, error->url, error->code);
        fflush(log_file_ptr);
    }
    
    if (config.debug_mode) {
        fprintf(stderr, "[%s] ERROR: %s\nEndpoint: %s\nURL: %s\nCode: %d\n",
                timestamp, error->message, error->endpoint, error->url, error->code);
    }
}

void handle_error(ErrorInfo *error) {
    log_error(error);
    if (error->code >= 500) {  // Server errors
        sleep(config.rate_limit * 2);  // Additional delay for server errors
    }
}

static size_t write_memory_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    MemoryStruct *mem = (MemoryStruct *)userp;
    
    char *ptr = realloc(mem->memory, mem->size + realsize + 1);
    if (!ptr) {
        fprintf(stderr, "Error: Not enough memory (realloc returned NULL)\n");
        return 0;
    }
    
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    
    return realsize;
}

int fetch_data_with_backoff(const char* url, MemoryStruct* chunk, int attempt, ErrorInfo *error) {
    if (attempt > 1) {
        int delay = (int)(pow(2, attempt - 1) * config.rate_limit);
        delay += rand() % config.rate_limit;  // Add jitter
        sleep(delay);
    }
    
    return fetch_data(url, chunk, error);
}

int fetch_data(const char* url, MemoryStruct* chunk, ErrorInfo *error) {
    CURLcode res;
    struct curl_slist *headers = NULL;
    long response_code = 0;
    
    const char* api_username = getenv("REPSLY_API_USERNAME");
    const char* api_password = getenv("REPSLY_API_PASSWORD");
    
    if (!api_username || !api_password) {
        if (error) {
            snprintf(error->message, MAX_ERROR_LENGTH, 
                    "API credentials not set. Please set REPSLY_API_USERNAME and REPSLY_API_PASSWORD environment variables.");
            error->code = -1;
        }
        return -1;
    }
    
    curl_easy_setopt(curl_handle, CURLOPT_URL, url);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, write_memory_callback);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)chunk);
    curl_easy_setopt(curl_handle, CURLOPT_USERNAME, api_username);
    curl_easy_setopt(curl_handle, CURLOPT_PASSWORD, api_password);
    curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, config.timeout);
    
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, headers);
    
    res = curl_easy_perform(curl_handle);
    
    curl_slist_free_all(headers);
    
    if (res != CURLE_OK) {
        if (error) {
            snprintf(error->message, MAX_ERROR_LENGTH, 
                    "CURL error: %s", curl_easy_strerror(res));
            error->code = res;
        }
        return -1;
    }
    
    curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &response_code);
    
    if (response_code != 200) {
        if (error) {
            snprintf(error->message, MAX_ERROR_LENGTH, 
                    "HTTP error: %ld", response_code);
            error->code = response_code;
        }
        return -1;
    }
    
    return 0;
}

void parse_command_line(int argc, char *argv[]) {
    static struct option long_options[] = {
        {"debug", no_argument, 0, 'd'},
        {"raw", no_argument, 0, 'R'},
        {"verbose", no_argument, 0, 'v'},
        {"endpoint", required_argument, 0, 'e'},
        {"output", required_argument, 0, 'o'},
        {"limit", required_argument, 0, 'l'},
        {"page-size", required_argument, 0, 'p'},
        {"from", required_argument, 0, 'f'},
        {"to", required_argument, 0, 't'},
        {"retries", required_argument, 0, 'r'},
        {"timeout", required_argument, 0, 'T'},
        {"no-cache", no_argument, 0, 'n'},
        {"update-cache", no_argument, 0, 'u'},
        {"format", required_argument, 0, 'F'},
        {"log", required_argument, 0, 'L'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "dRve:o:l:p:f:t:r:T:nuF:L:h", 
                             long_options, NULL)) != -1) {
        switch (opt) {
            case 'd':
                config.debug_mode = 1;
                break;
            case 'R':
                config.raw_data_mode = 1;
                break;
            case 'v':
                config.verbose_mode = 1;
                break;
            case 'e':
                config.specific_endpoint = strdup(optarg);
                break;
            case 'o':
                config.output_directory = strdup(optarg);
                break;
            case 'l':
                config.rate_limit = atoi(optarg);
                break;
            case 'p':
                config.page_size = atoi(optarg);
                break;
            case 'f':
                config.from_date = strdup(optarg);
                break;
            case 't':
                config.to_date = strdup(optarg);
                break;
            case 'r':
                config.retry_attempts = atoi(optarg);
                break;
            case 'T':
                config.timeout = atoi(optarg);
                break;
            case 'n':
                config.use_cache = 0;
                break;
            case 'u':
                config.update_cache = 1;
                break;
            case 'F':
                config.export_format = strdup(optarg);
                break;
            case 'L':
                config.log_file = strdup(optarg);
                break;
            case 'h':
                print_help();
                exit(0);
            default:
                fprintf(stderr, "Try '%s --help' for more information.\n", argv[0]);
                exit(1);
        }
    }
    
    validate_config();
}

void validate_config(void) {
    if (config.rate_limit < 0) {
        fprintf(stderr, "Error: Rate limit must be non-negative\n");
        exit(1);
    }
    
    if (config.page_size <= 0) {
        fprintf(stderr, "Error: Page size must be positive\n");
        exit(1);
    }
    
    if (config.retry_attempts < 0) {
        fprintf(stderr, "Error: Retry attempts must be non-negative\n");
        exit(1);
    }
    
    if (config.timeout <= 0) {
        fprintf(stderr, "Error: Timeout must be positive\n");
        exit(1);
    }
    
    if (config.export_format && 
        strcmp(config.export_format, "csv") != 0 && 
        strcmp(config.export_format, "json") != 0) {
        fprintf(stderr, "Error: Export format must be 'csv' or 'json'\n");
        exit(1);
    }
    
    if (config.specific_endpoint) {
        bool valid_endpoint = false;
        for (int i = 0; i < num_endpoints; i++) {
            if (strcmp(config.specific_endpoint, endpoints[i].name) == 0) {
                valid_endpoint = true;
                break;
            }
        }
        if (!valid_endpoint) {
            fprintf(stderr, "Error: Invalid endpoint specified\n");
            exit(1);
        }
    }
}

char* get_current_datetime(void) {
    time_t now = time(NULL);
    struct tm *t = gmtime(&now);
    char *datetime = malloc(MAX_DATE_LENGTH);
    if (!datetime) {
        fprintf(stderr, "Error: Failed to allocate memory for datetime\n");
        return NULL;
    }
    strftime(datetime, MAX_DATE_LENGTH - 1, "%Y-%m-%dT%H:%M:%SZ", t);
    return datetime;
}

char* convert_date(const char* date_string) {
    static char buffer[MAX_DATE_LENGTH];
    long long milliseconds;
    int timezone_offset;
    
    // Handle /Date()/ format
    if (sscanf(date_string, "/Date(%lld%d)/", &milliseconds, &timezone_offset) == 2) {
        time_t seconds = milliseconds / 1000;
        struct tm* tm_info = gmtime(&seconds);
        
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm_info);
        
        int hours = abs(timezone_offset) / 100;
        int minutes = abs(timezone_offset) % 100;
        snprintf(buffer + strlen(buffer), sizeof(buffer) - strlen(buffer), 
                " %c%02d:%02d", 
                timezone_offset >= 0 ? '+' : '-', 
                hours, 
                minutes);
        
        return buffer;
    }
    
    // Handle ISO 8601 format
    struct tm tm_info = {0};
    char *result = strptime(date_string, "%Y-%m-%dT%H:%M:%S", &tm_info);
    if (result) {
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_info);
        return buffer;
    }
    
    // Return original string if no conversion needed
    strncpy(buffer, date_string, sizeof(buffer) - 1);
    buffer[sizeof(buffer) - 1] = '\0';
    return buffer;
}

void split_date_range(const char *start_date, const char *end_date, 
                     char **current_start, char **current_end) {
    struct tm start_tm = {0}, end_tm = {0}, current_tm = {0};
    time_t start_time, end_time, current_time;
    
    // Parse start and end dates
    strptime(start_date, "%Y-%m-%d", &start_tm);
    strptime(end_date, "%Y-%m-%d", &end_tm);
    
    start_time = mktime(&start_tm);
    end_time = mktime(&end_tm);
    current_time = start_time;
    
    // Allocate memory for date strings
    *current_start = malloc(MAX_DATE_LENGTH);
    *current_end = malloc(MAX_DATE_LENGTH);
    if (!*current_start || !*current_end) {
        fprintf(stderr, "Error: Failed to allocate memory for date range\n");
        return;
    }
    
    // Convert current time to tm structure
    current_tm = *localtime(&current_time);
    
    // Format dates
    strftime(*current_start, MAX_DATE_LENGTH, "%Y-%m-%d", &current_tm);
    
    // Add default interval (7 days)
    current_time += (7 * 24 * 60 * 60);
    if (current_time > end_time) {
        current_time = end_time;
    }
    
    current_tm = *localtime(&current_time);
    strftime(*current_end, MAX_DATE_LENGTH, "%Y-%m-%d", &current_tm);
}


bool validate_response_format(const json_object *parsed_json, const Endpoint *endpoint) {
    if (!parsed_json || !endpoint) return false;
    
    struct json_object *items;
    if (!json_object_object_get_ex(parsed_json, endpoint->key, &items)) {
        if (config.debug_mode) {
            fprintf(stderr, "[%s] Key '%s' not found in response\n", 
                    endpoint->name, endpoint->key);
            fprintf(stderr, "Available keys: ");
            json_object_object_foreach(parsed_json, key, val) {
                fprintf(stderr, "%s ", key);
            }
            fprintf(stderr, "\n");
        }
        return false;
    }
    
    if (json_object_get_type(items) != json_type_array) {

        if (config.debug_mode) {
            fprintf(stderr, "[%s] '%s' is not an array\n", 
                    endpoint->name, endpoint->key);
        }
        return false;
    }
    
    if (endpoint->pagination_type != NONE) {
        struct json_object *meta;
        if (!json_object_object_get_ex(parsed_json, "MetaCollectionResult", &meta)) {
            if (config.debug_mode) {
                fprintf(stderr, "[%s] MetaCollectionResult not found\n", endpoint->name);
            }
            return false;
        }
        
        if (endpoint->pagination_type == TIMESTAMP || 
            endpoint->pagination_type == ID) {
            const char *field = endpoint->use_timestamp_pagination ? 
                              "LastTimeStamp" : "LastID";
            struct json_object *last_val;
            if (!json_object_object_get_ex(meta, field, &last_val)) {
                if (config.debug_mode) {
                    fprintf(stderr, "[%s] %s not found in metadata\n", 
                            endpoint->name, field);
                }
                return false;
            }
        }
    }
    
    return true;
}

bool is_valid_response(const char *response, const Endpoint *endpoint) {
    if (!response || !endpoint) return false;
    
    struct json_object *parsed_json = json_tokener_parse(response);
    if (!parsed_json) {
        if (config.debug_mode) {
            fprintf(stderr, "[%s] Failed to parse JSON response\n", endpoint->name);
        }
        return false;
    }
    
    bool is_valid = validate_response_format(parsed_json, endpoint);
    json_object_put(parsed_json);
    return is_valid;
}

bool update_pagination(const Endpoint *endpoint, json_object *parsed_json, 
                      char *last_id, size_t last_id_size, int *skip, 
                      char **begin_date, char **end_date) {
    struct json_object *meta;
    if (!json_object_object_get_ex(parsed_json, "MetaCollectionResult", &meta)) {
        return false;
    }
    
    switch (endpoint->pagination_type) {
        case TIMESTAMP:
            {
                struct json_object *last_timestamp_obj;
                const char *last_timestamp_key = "LastTimeStamp";
                
                if (!json_object_object_get_ex(meta, last_timestamp_key, &last_timestamp_obj)) {
                    return false;
                }
                
                long long new_timestamp = json_object_get_int64(last_timestamp_obj);
                long long current_timestamp;
                sscanf(last_id, "%lld", &current_timestamp);
                
                if (new_timestamp <= current_timestamp) {
                    return false;
                }
                
                snprintf(last_id, last_id_size, "%lld", new_timestamp);
                return true;
            }
            
        case ID:
            {
                struct json_object *last_id_obj;
                if (!json_object_object_get_ex(meta, "LastID", &last_id_obj)) {
                    return false;
                }
                
                const char *new_last_id = json_object_get_string(last_id_obj);
                if (strcmp(new_last_id, last_id) == 0) {
                    return false;
                }
                
                strncpy(last_id, new_last_id, last_id_size - 1);
                last_id[last_id_size - 1] = '\0';
                return true;
            }
            
        case SKIP:
            {
                struct json_object *total_obj;
                if (!json_object_object_get_ex(meta, "TotalCount", &total_obj)) {
                    return false;
                }
                
                int total = json_object_get_int(total_obj);
                *skip += config.page_size;
                
                return *skip < total;
            }
            
        case DATE_RANGE:
            {
                // Get current date range end
                time_t current_end_time;
                struct tm end_tm = {0};
                strptime(*end_date, "%Y-%m-%d", &end_tm);
                current_end_time = mktime(&end_tm);
                
                // Move to next date range
                free(*begin_date);
                *begin_date = strdup(*end_date);
                
                // Calculate new end date
                current_end_time += (endpoint->default_date_range_days * 24 * 60 * 60);
                
                // Check if we've reached the final end date
                time_t final_end_time;
                struct tm final_end_tm = {0};
                strptime(config.to_date ? config.to_date : get_current_datetime(), 
                        "%Y-%m-%d", &final_end_tm);
                final_end_time = mktime(&final_end_tm);
                
                if (current_end_time >= final_end_time) {
                    return false;
                }
                
                // Format new end date
                struct tm *new_end_tm = localtime(&current_end_time);
                char new_end_date[MAX_DATE_LENGTH];
                strftime(new_end_date, sizeof(new_end_date), "%Y-%m-%d", new_end_tm);
                
                free(*end_date);
                *end_date = strdup(new_end_date);
                
                return true;
            }
            
        default:
            return false;
    }
}

void process_json_value(FILE *csv_file, struct json_object *val, bool convert_timestamp) {
    enum json_type type = json_object_get_type(val);
    switch (type) {
        case json_type_null:
            fputs("", csv_file);
            break;
            
        case json_type_boolean:
            fprintf(csv_file, json_object_get_boolean(val) ? "true" : "false");
            break;
            
        case json_type_double:
            fprintf(csv_file, "%.6f", json_object_get_double(val));
            break;
            
        case json_type_int:
            fprintf(csv_file, "%d", json_object_get_int(val));
            break;
            
        case json_type_string: {
            const char *str = json_object_get_string(val);
            if (convert_timestamp && strstr(str, "/Date(") == str) {
                fprintf(csv_file, "%s", convert_date(str));
            } else if (strchr(str, ',') != NULL || strchr(str, '"') != NULL) {
                fprintf(csv_file, "\"%s\"", str);
            } else {
                fprintf(csv_file, "%s", str);
            }
            break;
        }
            
        case json_type_array:
        case json_type_object:
            {
                const char *json_str = json_object_to_json_string(val);
                fprintf(csv_file, "\"%s\"", json_str);
            }
            break;
    }
}

bool is_duplicate_record(json_object *item, const char *last_id, bool use_timestamp) {
    struct json_object *id_obj;
    const char *id_field = use_timestamp ? "TimeStamp" : "ID";
    
    if (!json_object_object_get_ex(item, id_field, &id_obj)) {
        return false;
    }
    
    if (use_timestamp) {
        long long item_timestamp = json_object_get_int64(id_obj);
        long long last_timestamp;
        sscanf(last_id, "%lld", &last_timestamp);
        return item_timestamp <= last_timestamp;
    } else {
        const char *item_id = json_object_get_string(id_obj);
        return strcmp(item_id, last_id) <= 0;
    }
}


void write_csv_header(FILE *csv_file, json_object *items, CSVState *csv_state) {
    if (!csv_file || !items || !csv_state) return;
    
    csv_state->headers = NULL;
    csv_state->header_count = 0;
    
    int array_len = json_object_array_length(items);
    if (array_len == 0) return;
    
    for (int i = 0; i < array_len; i++) {
        json_object *item = json_object_array_get_idx(items, i);
        json_object_object_foreach(item, key, val) {
            bool found = false;
            for (int j = 0; j < csv_state->header_count; j++) {
                if (strcmp(csv_state->headers[j], key) == 0) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                csv_state->header_count++;
                csv_state->headers = realloc(csv_state->headers, 
                    csv_state->header_count * sizeof(char*));
                if (!csv_state->headers) {
                    fprintf(stderr, "Error: Memory allocation failed for headers\n");
                    return;
                }
                csv_state->headers[csv_state->header_count - 1] = strdup(key);
            }
        }
    }
    
    for (int i = 0; i < csv_state->header_count - 1; i++) {
        for (int j = i + 1; j < csv_state->header_count; j++) {
            if (strcmp(csv_state->headers[i], csv_state->headers[j]) > 0) {
                char *temp = csv_state->headers[i];
                csv_state->headers[i] = csv_state->headers[j];
                csv_state->headers[j] = temp;
            }
        }
    }
    
    csv_state->last_row_values = calloc(csv_state->header_count * MAX_BUFFER, 
                                      sizeof(char));
    if (!csv_state->last_row_values) {
        fprintf(stderr, "Error: Memory allocation failed for row values\n");
        return;
    }
    csv_state->last_row_size = csv_state->header_count * MAX_BUFFER;
    
    for (int i = 0; i < csv_state->header_count; i++) {
        if (i > 0) fprintf(csv_file, ",");
        
        if (strchr(csv_state->headers[i], ',') || strchr(csv_state->headers[i], '"')) {
            fprintf(csv_file, "\"%s\"", csv_state->headers[i]);
        } else {
            fprintf(csv_file, "%s", csv_state->headers[i]);
        }
    }
    fprintf(csv_file, "\n");
    fflush(csv_file);
}

void write_csv_row(FILE *csv_file, json_object *item, bool convert_timestamp, 
                  CSVState *csv_state) {
    if (!csv_file || !item || !csv_state) return;
    
    memset(csv_state->last_row_values, 0, csv_state->last_row_size);
    
    for (int i = 0; i < csv_state->header_count; i++) {
        if (i > 0) fprintf(csv_file, ",");
        
        struct json_object *val = NULL;
        if (json_object_object_get_ex(item, csv_state->headers[i], &val)) {
            char buffer[MAX_BUFFER] = {0};
            FILE *temp = fmemopen(buffer, sizeof(buffer), "w");
            if (temp) {
                process_json_value(temp, val, convert_timestamp);
                fclose(temp);
                
                strncpy(csv_state->last_row_values + (i * MAX_BUFFER), 
                        buffer, MAX_BUFFER - 1);
                
                fprintf(csv_file, "%s", buffer);
            }
        }
    }
    
    fprintf(csv_file, "\n");
    fflush(csv_file);
}

void escape_csv_string(const char *input, char *output, size_t output_size) {
    if (!input || !output || output_size == 0) return;
    
    bool needs_quotes = false;
    const char *p = input;
    size_t out_pos = 0;
    
    if (strchr(input, ',') || strchr(input, '"') || strchr(input, '\n') || 
        strchr(input, '\r')) {
        needs_quotes = true;
    }
    
    if (needs_quotes && out_pos < output_size - 1) {
        output[out_pos++] = '"';
    }
    
    while (*p && out_pos < output_size - 2) {  // -2 for closing quote and null terminator
        if (*p == '"') {
            if (out_pos < output_size - 3) {  // Need space for two quotes
                output[out_pos++] = '"';
                output[out_pos++] = '"';
            } else {
                break;
            }
        } else {
            output[out_pos++] = *p;
        }
        p++;
    }
    
    if (needs_quotes && out_pos < output_size - 1) {
        output[out_pos++] = '"';
    }
    
    output[out_pos] = '\0';
}

void cleanup_csv_state(CSVState *csv_state) {
    if (!csv_state) return;
    
    if (csv_state->headers) {
        for (int i = 0; i < csv_state->header_count; i++) {
            free(csv_state->headers[i]);
        }
        free(csv_state->headers);
        csv_state->headers = NULL;
    }
    
    if (csv_state->last_row_values) {
        free(csv_state->last_row_values);
        csv_state->last_row_values = NULL;
    }
    
    csv_state->header_count = 0;
    csv_state->last_row_size = 0;
}

bool validate_csv_file(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) return false;
    
    char line[MAX_BUFFER];
    int line_count = 0;
    int field_count = -1;
    bool is_valid = true;
    
    while (fgets(line, sizeof(line), file) && line_count < 10) {
        line[strcspn(line, "\n")] = 0;
        
        int current_fields = 1;  // Start with 1 for the first field
        bool in_quotes = false;
        
        for (char *p = line; *p; p++) {
            if (*p == '"') {
                in_quotes = !in_quotes;
            } else if (*p == ',' && !in_quotes) {
                current_fields++;
            }
        }
        
        if (field_count == -1) {
            field_count = current_fields;
        } else if (field_count != current_fields) {
            is_valid = false;
            break;
        }
        
        line_count++;
    }
    
    fclose(file);
    return is_valid && line_count > 0;
}

void merge_csv_files(const char *input_file, const char *output_file, bool append) {
    FILE *in = fopen(input_file, "r");
    if (!in) return;
    
    FILE *out = fopen(output_file, append ? "a" : "w");
    if (!out) {
        fclose(in);
        return;
    }
    
    char line[MAX_BUFFER];
    bool is_header = true;
    
    while (fgets(line, sizeof(line), in)) {
        if (is_header && append) {
            is_header = false;
            continue;  // Skip header when appending
        }
        fputs(line, out);
    }
    
    fclose(in);
    fclose(out);
}


int process_endpoint(const Endpoint *endpoint) {
    if (!endpoint) return -1;
    
    char url[MAX_URL_LENGTH];
    MemoryStruct chunk = {0};
    char last_id[64] = "0";
    size_t last_id_size = sizeof(last_id);
    char filename[MAX_URL_LENGTH];
    char cache_filename[MAX_URL_LENGTH];
    PaginationState pagination = {0};
    int skip = 0;
    char *begin_date = NULL;
    char *end_date = NULL;
    FILE *output_file = NULL;
    int result = 0;
    CSVState csv_state = {0};
    ErrorInfo error = {0};
    bool first_batch = true;
    
    chunk.memory = malloc(1);
    if (!chunk.memory) {
        log_message("Failed to allocate initial memory for %s", endpoint->name);
        return -1;
    }
    chunk.size = 0;
    
    if (config.output_directory) {
        snprintf(filename, sizeof(filename), "%s/Repsly_%s_Export.%s", 
                config.output_directory, endpoint->key, config.export_format);
        snprintf(cache_filename, sizeof(cache_filename), "%s/Repsly_%s_cache.txt", 
                config.output_directory, endpoint->key);
    } else {
        snprintf(filename, sizeof(filename), "Repsly_%s_Export.%s", 
                endpoint->key, config.export_format);
        snprintf(cache_filename, sizeof(cache_filename), "Repsly_%s_cache.txt", 
                endpoint->key);
    }
    
    if (endpoint->pagination_type != NONE && config.use_cache) {
        if (!load_cache(cache_filename, last_id, last_id_size)) {
            if (endpoint->pagination_type == TIMESTAMP) {
                strcpy(last_id, "0");
            }
        }
    }
    
    output_file = fopen(filename, strcmp(config.export_format, "csv") == 0 ? "w" : "a");
    if (!output_file) {
        log_message("Failed to open output file %s for %s", filename, endpoint->name);
        free(chunk.memory);
        free(begin_date);
        free(end_date);
        return -1;
    }
    
    if (strcmp(config.export_format, "json") == 0) {
        fprintf(output_file, "{\n\"%s\": [\n", endpoint->key);
    }
    
    if (config.debug_mode) {
        log_message("Starting processing for %s", endpoint->name);
    }
    if (config.from_date) {
    begin_date = strdup(config.from_date);
    } else {
        time_t now = time(NULL);
        time_t start = now - (endpoint->default_date_range_days * 24 * 60 * 60);
        struct tm *tm_start = localtime(&start);
        begin_date = malloc(MAX_DATE_LENGTH);
        if (begin_date) {
            strftime(begin_date, MAX_DATE_LENGTH, "%Y-%m-%d", tm_start);
        }
    }
    
    if (config.to_date) {
        end_date = strdup(config.to_date);
    } else {
        end_date = get_current_datetime();
    }

    if (!begin_date || !end_date) {
        log_message("[%s] Failed to initialize date range", endpoint->name);
        result = -1;
        goto cleanup;
    }
    pagination.page_number = 1;
    pagination.has_more = true;
    
    while (pagination.has_more && pagination.page_number < config.max_iterations) {
        construct_url(url, sizeof(url), endpoint, last_id, skip, begin_date, end_date, &error);
        if (error.code != 0) {
            handle_error(&error);
            break;
        }
        
        add_query_parameters(url, sizeof(url), endpoint);
        
        if (config.verbose_mode) {
            log_message("[%s] Requesting page %d (processed %d records)", 
                       endpoint->name, pagination.page_number, pagination.records_processed);
            if (config.debug_mode) {
                log_message("URL: %s", url);
            }
        }
        
        sleep(config.rate_limit);
        
        bool fetch_success = false;
        for (int retry = 0; retry < config.retry_attempts; retry++) {
            if (fetch_data_with_backoff(url, &chunk, retry + 1, &error) == 0) {
                fetch_success = true;
                break;
            }
            
            handle_error(&error);
            if (error.code >= 400 && error.code < 500) break; // Don't retry client errors
        }
        
        if (!fetch_success) {
            log_message("[%s] Failed to fetch data after %d attempts", 
                       endpoint->name, config.retry_attempts);
            break;
        }
        
        if (config.raw_data_mode) {
            printf("[%s] Raw response:\n%s\n", endpoint->name, chunk.memory);
            break;
        }
        
        struct json_object *parsed_json = json_tokener_parse(chunk.memory);
        if (!parsed_json || !validate_response_format(parsed_json, endpoint)) {
            log_message("[%s] Invalid JSON response", endpoint->name);
            if (config.debug_mode && parsed_json) {
                log_message("Response: %s", json_object_to_json_string(parsed_json));
            }
            if (parsed_json) json_object_put(parsed_json);
            break;
        }
        
        struct json_object *items;
        json_object_object_get_ex(parsed_json, endpoint->key, &items);
        int n_items = json_object_array_length(items);
        
        if (config.verbose_mode) {
            log_message("[%s] Received %d records", endpoint->name, n_items);
        }
        
        if (n_items == 0) {
            if (config.verbose_mode) {
                log_message("[%s] No more records to process", endpoint->name);
            }
            json_object_put(parsed_json);
            break;
        }
        
        if (strcmp(config.export_format, "csv") == 0) {
            if (first_batch) {
                write_csv_header(output_file, items, &csv_state);
                first_batch = false;
            }
            
            for (int i = 0; i < n_items; i++) {
                struct json_object *item = json_object_array_get_idx(items, i);
                if (!is_duplicate_record(item, last_id, endpoint->use_timestamp_pagination)) {
                    write_csv_row(output_file, item, !endpoint->use_raw_timestamp, &csv_state);
                    pagination.records_processed++;
                } else if (config.debug_mode) {
                    log_message("[%s] Skipping duplicate record", endpoint->name);
                }
            }
        } else if (strcmp(config.export_format, "json") == 0) {
            for (int i = 0; i < n_items; i++) {
                if (!first_batch || i > 0) {
                    fprintf(output_file, ",\n");
                }
                fprintf(output_file, "%s", 
                        json_object_to_json_string_ext(
                            json_object_array_get_idx(items, i), 
                            JSON_C_TO_STRING_PRETTY));
                first_batch = false;
            }
        }
        
        if (!update_pagination(endpoint, parsed_json, last_id, last_id_size, 
                             &skip, &begin_date, &end_date)) {
            if (config.verbose_mode) {
                log_message("[%s] Pagination complete", endpoint->name);
            }
            pagination.has_more = false;
        }
        
        json_object_put(parsed_json);
        
        free(chunk.memory);
        chunk.memory = malloc(1);
        if (!chunk.memory) {
            log_message("[%s] Failed to allocate memory for chunk", endpoint->name);
            break;
        }
        chunk.size = 0;
        
        pagination.page_number++;
    }
    
    if (pagination.page_number >= config.max_iterations) {
        log_message("[%s] Reached maximum iteration limit (%d)", 
                   endpoint->name, config.max_iterations);
    }
    
    if (config.verbose_mode) {
        log_message("\n[%s] Complete - Processed %d records in %d pages\n",
                   endpoint->name, pagination.records_processed, pagination.page_number);
    }
    
    if (endpoint->pagination_type != NONE && config.use_cache && config.update_cache) {
        save_cache(cache_filename, last_id);
    }
    
    if (strcmp(config.export_format, "json") == 0) {
        fprintf(output_file, "\n]\n}");
    }
    
    if (output_file) fclose(output_file);
    free(chunk.memory);
    free(begin_date);
    free(end_date);
    cleanup_csv_state(&csv_state);
    
    return result;
}
void construct_url(char *url, size_t url_size, const Endpoint *endpoint, 
                  const char *last_id, int skip, const char *begin_date, 
                  const char *end_date, ErrorInfo *error) {
    if (!url || !endpoint) {
        if (error) {
            snprintf(error->message, MAX_ERROR_LENGTH, "Invalid parameters for URL construction");
            error->code = -1;
        }
        return;
    }

    if (endpoint->pagination_type == DATE_RANGE && (!begin_date || !end_date)) {
        if (error) {
            snprintf(error->message, MAX_ERROR_LENGTH, 
                    "Missing date parameters for date-range pagination");
            error->code = -1;
        }
        return;
    }

    switch (endpoint->pagination_type) {
        case NONE:
        case TIMESTAMP:
        case ID:
            snprintf(url, url_size, endpoint->url_format, last_id);
            break;
            
        case SKIP:
            snprintf(url, url_size, endpoint->url_format, last_id, skip);
            break;
            
        case DATE_RANGE:
            snprintf(url, url_size, endpoint->url_format, begin_date, end_date);
            break;
    }
}

void add_query_parameters(char *url, size_t url_size, const Endpoint *endpoint) {
    if (!url || !endpoint) return;
    
    char *separator = strchr(url, '?') ? "&" : "?";
    
    if (endpoint->required_parameters) {
        snprintf(url + strlen(url), url_size - strlen(url), 
                "%s%s", separator, endpoint->required_parameters);
        separator = "&";
    }
    
    if (endpoint->include_inactive) {
        snprintf(url + strlen(url), url_size - strlen(url), 
                "%sincludeInactive=true", separator);
        separator = "&";
    }
    
    if (endpoint->include_deleted) {
        snprintf(url + strlen(url), url_size - strlen(url), 
                "%sincludeDeleted=true", separator);
    }
}

bool load_cache(const char *cache_filename, char *last_id, size_t last_id_size) {
    if (!cache_filename || !last_id) return false;
    
    FILE *cache_file = fopen(cache_filename, "r");
    if (!cache_file) {
        if (config.debug_mode) {
            log_message("No cache found at %s", cache_filename);
        }
        return false;
    }
    
    if (fgets(last_id, last_id_size, cache_file) == NULL) {
        if (config.debug_mode) {
            log_message("Error reading from cache file %s", cache_filename);
        }
        fclose(cache_file);
        return false;
    }
    
    last_id[strcspn(last_id, "\n")] = 0;
    
    if (config.debug_mode) {
        log_message("Loaded cache value: %s", last_id);
    }
    
    fclose(cache_file);
    return true;
}

void save_cache(const char *cache_filename, const char *last_id) {
    if (!cache_filename || !last_id) return;
    
    FILE *cache_file = fopen(cache_filename, "w");
    if (!cache_file) {
        log_message("Error: Unable to save cache file %s", cache_filename);
        return;
    }
    
    fprintf(cache_file, "%s", last_id);
    fclose(cache_file);
    
    if (config.debug_mode) {
        log_message("Saved cache value: %s", last_id);
    }
}

void log_message(const char *format, ...) {
    if (!format) return;
    
    time_t now = time(NULL);
    char timestamp[26];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));
    
    va_list args;
    va_start(args, format);
    
    if (log_file_ptr) {
        fprintf(log_file_ptr, "[%s] ", timestamp);
        vfprintf(log_file_ptr, format, args);
        fprintf(log_file_ptr, "\n");
        fflush(log_file_ptr);
    }
    
    if (config.verbose_mode || config.debug_mode) {
        fprintf(stderr, "[%s] ", timestamp);
        vfprintf(stderr, format, args);
        fprintf(stderr, "\n");
    }
    
    va_end(args);
}

void print_help(void) {
    printf("Usage: repsly2csv [OPTIONS]\n");
    printf("Options:\n");
    printf("  -d, --debug             Enable debug mode\n");
    printf("  -R, --raw               Output raw JSON data\n");
    printf("  -v, --verbose           Enable verbose output\n");
    printf("  -e, --endpoint ENDPOINT Specify a single endpoint to process\n");
    printf("  -o, --output DIR        Specify output directory\n");
    printf("  -l, --limit SECONDS     Set rate limit in seconds (default: 1)\n");
    printf("  -p, --page-size SIZE    Set page size for requests (default: 50)\n");
    printf("  -f, --from DATE         Start date for data retrieval\n");
    printf("  -t, --to DATE           End date for data retrieval\n");
    printf("  -r, --retries NUM       Number of retry attempts (default: 3)\n");
    printf("  -T, --timeout SECONDS   Request timeout in seconds (default: 30)\n");
    printf("  -n, --no-cache          Disable caching\n");
    printf("  -u, --update-cache      Force cache update\n");
    printf("  -F, --format FORMAT     Export format (csv, json) (default: csv)\n");
    printf("  -L, --log FILE          Specify log file\n");
    printf("  -h, --help              Display this help message\n");
}

int main(int argc, char *argv[]) {
    int result = 0;
    
    config.rate_limit = DEFAULT_RATE_LIMIT_SECONDS;
    config.page_size = DEFAULT_PAGE_SIZE;
    config.retry_attempts = DEFAULT_RETRY_ATTEMPTS;
    config.timeout = DEFAULT_TIMEOUT;
    config.use_cache = 1;
    config.export_format = "csv";
    config.max_iterations = DEFAULT_MAX_ITERATIONS;
    config.debug_mode = 0;
    config.raw_data_mode = 0;
    config.verbose_mode = 0;
    config.specific_endpoint = NULL;
    config.output_directory = NULL;
    config.from_date = NULL;
    config.to_date = NULL;
    config.update_cache = 0;
    config.log_file = NULL;
    
    parse_command_line(argc, argv);
    initialize_app();
    
    for (int i = 0; i < num_endpoints; i++) {
        if (config.specific_endpoint && 
            strcmp(config.specific_endpoint, endpoints[i].name) != 0) {
            continue;  // Skip non-matching endpoints
        }
        
        if (process_endpoint(&endpoints[i]) != 0) {
            log_message("Error processing endpoint: %s", endpoints[i].name);
            result = 1;
        }
    }
    
    cleanup_app();
    
    return result;
}
