namespace scala com.github.ponkin.bloom.driver

enum FilterType {
  STANDART,
  STABLE,
  SCALABLE,
  CUCKOO,
  COUNTING
}

struct Filter {
  1: i64 numEntries,
  2: double fpProbe = 0.02,
  3: FilterType filterType,
  4: map<string, string> params
}

service BloomFilterStore {

  oneway void create(1:string name, 2:Filter filter)

  oneway void destroy(1:string name)
  
  oneway void put(1:string name, 2:set<string> elements)

  oneway void remove(1:string name, 2:set<string> elements)
  
  oneway void clear(1:string name)

  bool mightContain(1:string name, 2:string element)

  string info(1:string name)

}
