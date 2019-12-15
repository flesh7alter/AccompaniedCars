#include <vector>
#include <iostream>
#include <fstream>
#include <map>
#include <set>
using namespace std;

map<int,string> _map;

void SplitString(string str, vector<string>& v, string c)
{
    int pos1,pos2;
    pos2 = str.find(c);
    pos1 = 0;
    while(std::string::npos != pos2)
    {
        v.push_back(str.substr(pos1, pos2-pos1));
    
        pos1 = pos2 + c.size();
        pos2 = str.find(c, pos1);
    }
    if(pos1 != str.length())
        v.push_back(str.substr(pos1));
}

class DisjSet
{
  private:
    vector<int> parent;
    vector<int> rank;

  public:
    DisjSet(int max_size) : parent(vector<int>(max_size)),
                            rank(vector<int>(max_size, 0))
    {
        for (int i = 0; i < max_size; ++i)
            parent[i] = i;
    }
    int find(int x)
    {
        return x == parent[x] ? x : (parent[x] = find(parent[x]));
    }
    void to_union(int x1, int x2)
    {
        int f1 = find(x1);
        int f2 = find(x2);
        if (rank[f1] > rank[f2])
            parent[f2] = f1;
        else
        {
            parent[f1] = f2;
            if (rank[f1] == rank[f2])
                ++rank[f2];
        }
    }
    bool is_same(int e1, int e2)
    {
        return find(e1) == find(e2);
    }
    void save(string count){
        ofstream outFile("tuples.txt",ios::app);
        int size = parent.size();
        map<int, vector<int> > ret;
        for(int i = 0; i != size; i++){
            if(parent[i] != i){
                int root = find(i);
                if(ret.find(root) == ret.end()){
                    vector<int> a;
                    ret[root] = a;
                }
                ret[root].push_back(i);
            }
        }
        for(map<int, vector<int> >::iterator iter = ret.begin(); iter != ret.end(); iter++){
            outFile<<"["<<_map[iter->first];
            int i = 0;
            for(;i!=iter->second.size();i++){
                outFile<<","<<_map[iter->second[i]];
            }
            outFile<<"]"<<count<<endl;
        }
    }
};


int main(){
    ifstream inFile("result-all.txt", ios::in);
	string str;
	map<string,int> mp;    
    string count = "0";
    DisjSet* dst;
    int n = 0;
    int tuple = 0;
    while (getline(inFile, str)){
        vector<string> record;
        SplitString(str.substr(1,str.size()-3),record,",");
        string car1 = record[0];
        string car2 = record[1];
        if(count!= "0" && count !=  record[2]){
            dst->save(count);
            count =  record[2];
            delete dst;
            dst = new DisjSet(500);
            mp.clear();
            _map.clear();
            n=0;
        }
        else if(count == "0"){
            count =  record[2];
            dst = new DisjSet(500);
            tuple = 0;
        }
        if(mp.find(car1) == mp.end()){
            mp[car1] = n;
            _map[n] = car1;
            n++;
        }
        if(mp.find(car2) == mp.end()){
            mp[car2] = n;
            _map[n] = car2;
            n++;
        }
        if(!dst->is_same(mp[car1],mp[car2])){
            dst->to_union(mp[car1],mp[car2]);
        }
        
    }

}