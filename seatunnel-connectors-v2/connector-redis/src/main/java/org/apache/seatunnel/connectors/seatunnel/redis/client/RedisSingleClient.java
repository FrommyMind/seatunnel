/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.redis.client;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.apache.commons.collections4.CollectionUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

// In standalone mode, pipeline can be used to improve batch read performance
public class RedisSingleClient extends RedisClient {

    public RedisSingleClient(RedisParameters redisParameters, Jedis jedis, int redisVersion) {
        super(redisParameters, jedis, redisVersion);
    }

    @Override
    public List<String> batchGetString(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        String[] keyArr = keys.toArray(new String[0]);
        return jedis.mget(keyArr);
    }

    @Override
    public List<Map<String, String>> batchGetStringWithKey(
            List<String> keys, String outputKeyName) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        String[] keyArr = keys.toArray(new String[0]);
        List<Map<String, String>> result = new ArrayList<>(keys.size());
        for (String key : keyArr) {
            result.add(Collections.singletonMap(key, jedis.get(key)));
        }

        return result;
    }

    @Override
    public List<List<String>> batchGetList(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<List<String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(pipeline.lrange(key, 0, -1));
        }

        pipeline.sync();

        List<List<String>> resultList = new ArrayList<>(keys.size());
        for (Response<List<String>> response : responses) {
            resultList.add(response.get());
        }

        return resultList;
    }

    public List<Map<String, List<String>>> batchGetListWithKey(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Map<String, Response<List<String>>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(Collections.singletonMap(key, pipeline.lrange(key, 0, -1)));
        }

        pipeline.sync();

        List<Map<String, List<String>>> resultList = new ArrayList<>(keys.size());
        for (Map<String, Response<List<String>>> response : responses) {
            Set<String> responseKeys = response.keySet();
            for (String key : responseKeys) {
                resultList.add(Collections.singletonMap(key, response.get(key).get()));
            }
        }
        return resultList;
    }

    @Override
    public List<Set<String>> batchGetSet(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<Set<String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(pipeline.smembers(key));
        }

        pipeline.sync();

        List<Set<String>> resultList = new ArrayList<>(keys.size());
        for (Response<Set<String>> response : responses) {
            resultList.add(response.get());
        }

        return resultList;
    }

    @Override
    public List<Map<String, Set<String>>> batchGetSetWithKey(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Map<String, Response<Set<String>>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(Collections.singletonMap(key, pipeline.smembers(key)));
        }

        pipeline.sync();

        List<Map<String, Set<String>>> resultList = new ArrayList<>(keys.size());
        for (Map<String, Response<Set<String>>> response : responses) {
            for (String responseKey : response.keySet()) {
                resultList.add(
                        Collections.singletonMap(responseKey, response.get(responseKey).get()));
            }
        }

        return resultList;
    }

    @Override
    public List<Map<String, String>> batchGetHash(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<Map<String, String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            Response<Map<String, String>> response = pipeline.hgetAll(key);
            responses.add(response);
        }

        pipeline.sync();

        List<Map<String, String>> resultList = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Response<Map<String, String>> response = responses.get(i);
            Map<String, String> map = response.get();
            resultList.add(map);
        }

        return resultList;
    }

    @Override
    public List<Map<String, Map<String, String>>> batchGetHashWithKey(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Map<String, Response<Map<String, String>>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            Response<Map<String, String>> response = pipeline.hgetAll(key);
            responses.add(Collections.singletonMap(key, response));
        }

        pipeline.sync();

        List<Map<String, Map<String, String>>> resultList = new ArrayList<>(keys.size());
        for (Map<String, Response<Map<String, String>>> response : responses) {
            for (String hash_key : response.keySet()) {
                Map<String, String> map = response.get(hash_key).get();
                resultList.add(Collections.singletonMap(hash_key, map));
            }
        }

        return resultList;
    }

    @Override
    public List<List<String>> batchGetZset(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Response<List<String>>> responses = new ArrayList<>(keys.size());
        Pipeline pipelined = jedis.pipelined();
        for (String key : keys) {
            Response<List<String>> response = pipelined.zrange(key, 0, -1);
            responses.add(response);
        }
        pipelined.sync();
        List<List<String>> resultlist = new ArrayList<>(keys.size());
        for (Response<List<String>> response : responses) {
            resultlist.add(response.get());
        }
        return resultlist;
    }

    @Override
    public List<Map<String, List<String>>> batchGetZsetWithKey(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Map<String, Response<List<String>>>> responses = new ArrayList<>(keys.size());
        Pipeline pipelined = jedis.pipelined();
        for (String key : keys) {
            Response<List<String>> response = pipelined.zrange(key, 0, -1);
            responses.add(Collections.singletonMap(key, response));
        }
        pipelined.sync();
        List<Map<String, List<String>>> resultlist = new ArrayList<>(keys.size());
        for (Map<String, Response<List<String>>> response : responses) {
            List<String> responseList = new ArrayList<>(keys.size());
            for (String responseKey : response.keySet()) {
                responseList.addAll(response.get(responseKey).get());
                resultlist.add(Collections.singletonMap(responseKey, responseList));
            }
        }
        return resultlist;
    }

    @Override
    public void batchWriteString(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RowKind rowKind = rowKinds.get(i);
            String key = keys.get(i);
            String value = values.get(i);
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                pipelined.del(key);
            } else {
                pipelined.set(key, value);
                if (expireSeconds > 0) {
                    pipelined.expire(key, expireSeconds);
                }
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteList(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RowKind rowKind = rowKinds.get(i);
            String key = keys.get(i);
            String value = values.get(i);
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                pipelined.lrem(key, 1, value);
            } else {
                pipelined.lpush(key, value);
                if (expireSeconds > 0) {
                    pipelined.expire(key, expireSeconds);
                }
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteSet(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RowKind rowKind = rowKinds.get(i);
            String key = keys.get(i);
            String value = values.get(i);
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                pipelined.srem(key, value);
            } else {
                pipelined.sadd(key, value);
                if (expireSeconds > 0) {
                    pipelined.expire(key, expireSeconds);
                }
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteHash(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RowKind rowKind = rowKinds.get(i);
            String key = keys.get(i);
            String value = values.get(i);
            Map<String, String> fieldsMap = JsonUtils.toMap(value);
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                    pipelined.hdel(key, entry.getKey());
                }
            } else {
                pipelined.hset(key, fieldsMap);
                if (expireSeconds > 0) {
                    pipelined.expire(key, expireSeconds);
                }
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteZset(
            List<RowKind> rowKinds, List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RowKind rowKind = rowKinds.get(i);
            String key = keys.get(i);
            String value = values.get(i);
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                pipelined.zrem(key, value);
            } else {
                pipelined.zadd(key, 1, value);
                if (expireSeconds > 0) {
                    pipelined.expire(key, expireSeconds);
                }
            }
        }
        pipelined.sync();
    }
}
