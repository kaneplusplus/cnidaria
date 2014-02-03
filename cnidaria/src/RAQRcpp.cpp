#include <Rcpp.h>

#include "RedisAggregateQueue.hpp"

using namespace Rcpp;

//typedef RedisAggregateQueueEager<RawVector> RAQType;
typedef RedisAggregateQueue<RawVector> RAQType;

//' Create a redis aggregate queue.
//' 
//' @param ip The host address of the redis server.
//' @param port The port the redis server is listening on.
//' @return An external pointer to an instance of type RedisAggregateQueue.
//' @export
// [[Rcpp::export]]
XPtr<RAQType> makeRAQ(string ip, int port)
{
  return XPtr<RAQType>(new RAQType(ip, port, true));
}

//' Add a resource queue to listen on.
//'
//' @param raq The pointer to the RedisAggregateQueue
//' @param key The key of the list to listen on
//' @return TRUE if the operation was successful FALSE otherwise
//' @export
// [[Rcpp::export]]
bool addPop(XPtr<RAQType> raq, string key)
{
  return raq->add_pop(key);
}

//' Get the keys for the lists that the redis aggregate queue is listening on
//'
//' @param raq The redis aggregate queue
//' @return The keys corresponding to the redis lists that the raq is listening
//' on
//' @export
// [[Rcpp::export]]
CharacterVector popKeys(XPtr<RAQType> raq)
{
  CharacterVector cv(raq->pop_key_size());
  raq->pop_keys(cv.begin());
  return cv;
}

//' Remove a key from the listening key list
//' 
//' @param raq The redis aggregate queue
//' @param key The key to remove
//' @return TRUE if the 
//' @export
// [[Rcpp::export]]
bool remPop(XPtr<RAQType> raq, string key)
{
  return raq->remove_pop(key);
}

//' Block for the next message on the redis aggregate queue
//'
//' @param raq The redis aggregate queue
//' @param timeout The number of seconds to wait for the next message
//' @return NULL if the timeout is reached otherwise the next message
//' @export
// [[Rcpp::export]]
List nextRAQMessage(XPtr<RAQType> raq, int timeout)
{
  RAQType::QueueValueType qv = raq->next(static_cast<size_t>(timeout));
  if (qv.first == "")
    return List();
  return List::create(Named(qv.first) = qv.second);
}

//' Are there any messages in the redis aggregate queue
//'
//' @param raq The redis aggregate queue
//' @return TRUE if the queue is empty FALSE otherwise
//' @export
// [[Rcpp::export]]
bool empty(XPtr<RAQType> raq)
{
  return raq->empty();
}

// [[Rcpp::export]]
XPtr<PopListener> makePopListener(string ip, int port)
{
  redisContext *c = redisConnect(ip.c_str(), port);
  return XPtr<PopListener>(new PopListener(c), true);
}

// [[Rcpp::export]]
bool add(XPtr<PopListener> ppl, string key)
{
  return ppl->add(key);
}

// [[Rcpp::export]]
bool remove(XPtr<PopListener> ppl, string key)
{
  return ppl->remove(key);
}

// [[Rcpp::export]]
CharacterVector keys(XPtr<PopListener> ppl)
{
  CharacterVector cv(ppl->size());
  ppl->keys(cv.begin());
  return cv;
}

// [[Rcpp::export]]
int size(XPtr<PopListener> ppl)
{
  return ppl->size();
}

// [[Rcpp::export]]
List nextMessage(XPtr<PopListener> ppl)
{
  auto p = ppl->next<RawVector>();
  if (p.first.size() > 0)
    return List::create(Named(p.first) = p.second);
  return List::create();
}

