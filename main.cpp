#include <iostream>
#include <algorithm>
#include <queue>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
using namespace std;
inline std::chrono::high_resolution_clock::time_point get_current_time_fenced()
{
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto res_time = std::chrono::high_resolution_clock::now();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    return res_time;
}

template<class D>
inline long long to_us(const D& d)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

void write_in_file(map<string,int> &global_map,const string &file) {
    ofstream myfile;
    myfile.open (file);
    for(auto i = global_map.cbegin(); i != global_map.cend(); ++i){
        myfile << "\""<< i->first << "\"" << ": " << i->second << endl;
    }
    myfile.close();
}
void write_result_in_file(const string &result_file,long long int total_time){
    ofstream myfile;
    myfile.open (result_file,ios_base::app);
    myfile << total_time << endl;

}
void read_file(int &flag,size_t block_size,string nameOFfile,deque<vector<string>> &que_with_work,mutex &k,condition_variable &cv_read) {
    vector<string> words;
    ifstream myfile(nameOFfile);
    string line;
    size_t line_index = 0;
    while (getline(myfile, line)) {
            ++line_index;
            words.push_back(line);
        if (line_index == block_size) {
            unique_lock<mutex>ul(k);
            que_with_work.push_back(words);
            line_index = 0;
            words.clear();
            ul.unlock();
            cv_read.notify_one();
        }

    }
    if (line_index > 0)
    {
        unique_lock<mutex>lg(k);
        que_with_work.push_back(words);
        lg.unlock();

    }

    flag = 1;
    cv_read.notify_all();

}

void print_map(map<string,int> &global_map){
    for(auto i = global_map.cbegin(); i != global_map.cend(); ++i){
        cout << i->first<< " " << i->second << endl;
    }
}

map<string, int> counted_words(vector<string> &words){
    map<string, int> map1;
    for(int i = 0; i < words.size();++i){
        if (map1.find(words.at(i)) == map1.end()){
            map1.insert(make_pair(words.at(i),1));

        }
        else
        {
            map1[words.at(i)]++;
        }
    }
    return map1;
}

vector<string> to_vector(vector<string> &lines){
    vector<string> words;
    string o = "";
    for (int k = 0; k < lines.size(); ++k) {
        transform(lines[k].begin(), lines[k].end(), lines[k].begin(), ::tolower);
        for (int j = 0; j < lines[k].size(); ++j) {
            if(isspace(lines[k][j])){
                if (o.size() > 0) {
                    words.push_back(o);
                    o = "";
                }
            }
            else if (!ispunct(lines[k][j])) {
                o += lines[k][j];
            }
        }
        if (o.size() > 0) {
            words.push_back(o);
            o = "";
        }
    }

    return words;
}

void work_for_threads(int &flag,int &flag1,deque<vector<string>> &que_with_work,map<string,int> &global_map,mutex &m,mutex &k,deque<map<string,int>> &que_map,condition_variable &cv,condition_variable &cv_cout){
    while(1) {
        unique_lock<mutex>ul(k);
        if (!que_with_work.empty()) {
            flag1 = 0;
            vector<string> first_element = move(que_with_work.front());
            que_with_work.pop_front();
            ul.unlock();
            vector<string> local_vector = to_vector(first_element);
            map<string, int> local_map = counted_words(local_vector);
            m.lock();
            que_map.push_back(local_map);
            m.unlock();
            cv_cout.notify_one();
            ul.lock();
        }else if(flag){
            ul.unlock();
            flag1 = 1;
            break;
        } else {
            cv.wait(ul);
        }
    }
}

void join(vector<thread> &vector_threads){
    for(int i = 0; i < vector_threads.size();i++){
        vector_threads[i].join();
    }
}

void thread_map_cat(deque<map<string,int>> &que_map,map<string,int> &global_map,mutex &m,int &flag1,condition_variable &cv_count){
    while(1) {
        unique_lock<mutex>ul(m);
        if(!que_map.empty()) {
            map<string,int> local_map = (map<string, int> &&) que_map.front();
            que_map.pop_front();
            ul.unlock();
            for (map<string, int>::iterator it = local_map.begin(); it != local_map.end(); it++) {
                    global_map[it->first] += it->second;
            }
            ul.lock();
        }else if(flag1){
            ul.unlock();
            break;
        }else {
            cv_count.wait(ul);
        }
    }
}

void threads(int n,int block_size,const string &namefile,map<string,int> &global_map,mutex &m,mutex &M,mutex &k,condition_variable &cv_read,condition_variable &cv_count){
    deque<vector<string>> que_with_work;
    deque<map<string,int>> que_map;
    vector<thread> vector_threads;
    int flag,flag1 = 0;
    auto reading_start = get_current_time_fenced();
    thread t1(read_file,ref(flag),ref(block_size),cref(namefile),ref(que_with_work),ref(m),ref(cv_read));
    for(int i = 0; i < n; i++){
        vector_threads.push_back(thread(work_for_threads,ref(flag),ref(flag1),ref(que_with_work),ref(global_map), ref(m),ref(k),ref(que_map),ref(cv_read),ref(cv_count)));
    }
    vector_threads.push_back(thread(thread_map_cat,ref(que_map),ref(global_map),ref(m),ref(flag1),ref(cv_count)));
    t1.join();
    auto reading_finish = get_current_time_fenced();
    auto time_of_reading = reading_finish - reading_start;
    printf("Time of reading: %llu\n",to_us(time_of_reading));
    join(vector_threads);
}

void read_config(reference_wrapper<const string> nameFile, string &in_file, int &n_threads, int &block_size){
    vector<string> words;
    ifstream myfile(nameFile);
    string line;
    while (getline(myfile, line)){
        unsigned long int pos = line.find("=");
        if(strcmp(line.substr(0,pos).c_str(),"in_file") == 0){
            in_file = line.substr(pos + 2,line.size() - 1);
            in_file = in_file.substr(0,in_file.length() - 1);
        }else if(strcmp(line.substr(0,pos).c_str(),"n_threads") == 0){
            n_threads =  stoi(line.substr(pos+1));
        }else if(strcmp(line.substr(0,pos).c_str(),"block_size") == 0){
            block_size = stoi(line.substr(pos+1));
        }
    }
}

int main() {
    auto stage1_start = get_current_time_fenced();
    map<string,int> global_map;
    const string config_file = "config.txt";
    const string result_file = "result.txt";
    const string total_time_file = "total_times.txt";
    mutex m,f,k;
    condition_variable cv_read,cv_count;
    string in_file;
    int block_size;
    int n_threads;
    read_config(cref(config_file), in_file, n_threads, block_size);
    auto stag2_start = get_current_time_fenced();
    threads(ref(n_threads),ref(block_size),cref(in_file),ref(global_map),ref(m),ref(f),ref(k),ref(cv_read),ref(cv_count));
    auto stage2_finish = get_current_time_fenced();
    auto time_of_counting = stage2_finish - stag2_start;
    printf("Time of counting words: %llu\n",to_us(time_of_counting));
    write_in_file(global_map,cref(result_file));
    auto stage1_finish = get_current_time_fenced();
    auto total_time = stage1_finish - stage1_start;
    printf("Time of total work: %llu\n",to_us(total_time));
    write_result_in_file(cref(total_time_file),to_us(total_time));
    return 0;
}
