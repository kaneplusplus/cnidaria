#include <iostream>
#include <string>

#include <stdexcept>

#include <set>

#include <mutex>
#include <thread>
#include <chrono>
#include <queue>
#include <memory>
#include <functional>

#include <hiredis/hiredis.h>
#include <string.h>

using namespace std;
// Helper function for getting keys from maps. 
//

typedef vector<string> KeyVec;

template<typename T>
KeyVec get2nds( const T &m)
{
  vector<typename T::key_type> ret(m.size());
  unsigned long i=0;
  for (auto p : m)
    ret[i++] = p.first;
  return ret;
}

class KeyManager
{
  public:

    bool add(const string &key)
    {
      _set.insert(key);
      return true;
    }

    bool remove(const string &key)
    {
      _set.erase(key);
      return true;
    }

    template<typename IterType>
    IterType keys(IterType it)
    {
      return copy(_set.begin(), _set.end(), it);
    }

    size_t size() const
    {
      return _set.size();
    }
  
  protected:
    set<string> _set;
};

class PopListener : public KeyManager
{
  public:

    PopListener() {}

    PopListener(redisContext *c, bool lpop=true) : _c(c), _lpop(lpop) 
    {
      _cmd = string( (_lpop ? "LPOP " : "RPOP ") );
    }

    virtual ~PopListener() {}

    template<typename T>
    pair<string, T> next()
    {
      void *r;
      redisReply *reply;
      for (auto key : _set)
      {
        r = redisCommand(_c, (_cmd + key).c_str());
        if (!r)
          throw(runtime_error("Redis connection error"));

        reply = reinterpret_cast<redisReply*>(r);
        if (REDIS_REPLY_STRING == reply->type)
        {
          T ret(reply->len);
          copy(reply->str, reply->str+reply->len, ret.begin());
          return make_pair(key, ret);
        }
        else if (REDIS_REPLY_ERROR == reply->type)
        {
          cerr << reply->str << endl;
        }
        else if (REDIS_REPLY_NIL == reply->type)
        {
          // Don't do anything.
        }
        else
        {
          cerr << "Unknown message received." << endl;
        }
      }
      return make_pair("", T(0));
    }

  protected:
    redisContext *_c;
    bool _lpop;
    string _cmd;
};


/*
template<typename BufferType>
void FromRedisToQueue(redisContext *c, const string &cmd, 
  const string &keyName, queue<pair<string, BufferType> > &q, mutex &m)
{
  void *r;
  redisReply *reply;
  r = redisCommand(c, cmd.c_str());
  if (r) 
  {
    reply = reinterpret_cast<redisReply*>(r);
    if (REDIS_REPLY_STRING == reply->type)
    {
      BufferType ret(reply->len);
      copy(reply->str, reply->str+reply->len, ret.begin());
      lock_guard<mutex> lock(m);
      q.push(make_pair(keyName, ret));
    }
    else if (REDIS_REPLY_ERROR == reply->type)
    {
      cerr << reply->str << endl;
    }
    else if (REDIS_REPLY_NIL == reply->type)
    {
      // Don't do anything.
    }
    else
    {
      cerr << "Unknown message received." << endl;
    }
    freeReplyObject(reply);
  }
  else
  {
    cerr << "NULL return" << endl;
  }
}

struct RedisContextDeleter
{
  void operator()(redisContext *c)
  {
    redisFree(c);
  }
};
template<typename BufferType_>
class RedisAggregateQueueEager
{
  public: 
    typedef BufferType_ BufferType;
    typedef pair<string, BufferType> QueueValueType;
    typedef queue<QueueValueType> QueueType;
    typedef map<string, thread> SubMapType;
    typedef shared_ptr<redisContext> RedisContextPtr;
    typedef tuple<RedisContextPtr, thread, bool> ResourceTuple;
    typedef map<string, ResourceTuple> ResourceMap;
  
  public:
    RedisAggregateQueueEager(const string &ip, const int port,
      const bool lpop=true) : _ip(ip), _port(port), _lpop(lpop) {}

    virtual ~RedisAggregateQueueEager() 
    {
      _rm.erase(_rm.begin(), _rm.end());
    }

    bool add_pop(const string &key)
    {
      // Make the redis pop command string.
      auto it = _rm.find(key);
      if (it != _rm.end())
      {
        return false;
      }
      string cmd = string( (_lpop ? "LPOP " : "RPOP ") ) + key;
      auto npc = NewRedisContextPointer();
      _rm[key] = ResourceTuple(npc, 
        thread(FromRedisToQueue<BufferType>, npc.get(), cref(cmd), 
          cref(key), ref(_q), ref(_m)), 
        true);
      return true;
    }

    template <typename IterType>
    IterType pop_keys(IterType it)
    {
      auto itr = _rm.begin();
      while (itr != _rm.end())
      {
        if (get<2>(itr->second) == true)
        {
          *it = itr->first;
          ++itr;
        }
        ++it;
      }
      return it;
    }

    size_t pop_key_size()
    {
      size_t ret = 0;
      auto itr = _rm.begin();
      while (itr != _rm.end())
      {
        if (get<2>(itr->second))
        {
          ++ret;
        }
        ++itr;
      }
      return ret;
    }

    bool remove_pop(const string &key)
    {
      auto it = _rm.find(key);
      if (it != _rm.end())
      {
        if (get<2>(it->second))
        {
          _rm.erase(it);
        }
      }
      return true;
    }
    
    bool empty() const
    {
      lock_guard<mutex> lock(_m);
      return _q.size() == 0;
    }


    QueueValueType next(size_t timeout=30, size_t sleepms = 100)
    {
      chrono::time_point<chrono::system_clock> start, end;
      start = std::chrono::system_clock::now();
      end = start;
      while (empty() && 
        chrono::duration_cast<chrono::seconds>(end-start).count() < 
        static_cast<long>(timeout))
      {
        this_thread::sleep_for( chrono::milliseconds(sleepms) );
        end = std::chrono::system_clock::now();
      }
      lock_guard<mutex> lock(_m);
      if (_q.empty())
        return QueueValueType();
      auto ret = _q.front();
      _q.pop();
      return ret;
    }

  protected:
    RedisContextPtr NewRedisContextPointer()
    {
      return RedisContextPtr(redisConnect(_ip.c_str(), _port), 
        RedisContextDeleter());
    }  
  
  protected:
    string _ip;
    int _port;
    bool _lpop;
    queue<QueueValueType> _q;
    ResourceMap _rm;
    mutable mutex _m;
};
*/

template<typename BufferType_>
class RedisAggregateQueue
{
  public:
    typedef BufferType_ BufferType;
    typedef pair<string, BufferType> QueueValueType;
    typedef queue<QueueValueType> QueueType;

  public:
      RedisAggregateQueue(const string &ip, const int port, 
        const bool lpop=true) : _ip(ip), _port(port), _lpop(lpop)
      {
        // Create the pop listener.
        _c = redisConnect(_ip.c_str(), port);
        if (_c->err)
          throw(runtime_error(_c->errstr));
        
        _pl = PopListener(_c, true);
      }

      virtual ~RedisAggregateQueue() 
      {
        if (_c) {redisFree(_c);}
      }

    // Add a new redis queue to pop from.
    bool add_pop(const string &key) 
    {
      return _pl.add(key);
    }
  
    // Get the keys for the pop queues.
    template <typename IterType>
    IterType pop_keys(IterType it) 
    {
      return _pl.keys(it);
    }
 
    size_t pop_key_size() 
    {
      return _pl.size();
    }
    
    // Don't request tasks on a specified queue.
    bool remove_pop(const string &key) 
    {
      return _pl.remove(key);
    }

    bool empty() const
    {
      lock_guard<mutex> lock(_mtx);
      return _q.size() == 0;
    }

    // Get the next queue item. If there isn't one then block for
    // timeout seconds or until a new task is queued.
    QueueValueType next(size_t timeout=30, size_t sleepms = 100)
    {
      chrono::time_point<chrono::system_clock> start, end;
      start = std::chrono::system_clock::now();
      end = start;
      while (empty() && 
        (chrono::duration_cast<chrono::seconds>(end-start).count() < 
          static_cast<long>(timeout)))
      { 
        QueueValueType rt = _pl.next<typename QueueValueType::second_type>();
        if (rt.first != "") 
        {
          push(rt);
          break;
        }
        //std::this_thread::sleep_for( chrono::milliseconds(sleepms) );
        end = std::chrono::system_clock::now();
      }

      lock_guard<mutex> lock(_mtx);
      if (_q.empty())
        return QueueValueType();
      auto ret = _q.front();
      _q.pop();
      return ret;
    }

  protected:

    void push(const QueueValueType &val)
    {
      lock_guard<mutex> lock(_mtx);
      _q.push(val);
    }

    PopListener _pl;
    QueueType _q;   
    mutable mutex _mtx;
    string _ip;
    int _port;
    redisContext *_c;
    bool _lpop;
};

