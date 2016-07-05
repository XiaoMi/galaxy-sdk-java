/**
 * Copyright 2016, Xiaomi.
 * All rights reserved.
 * Author: xiajun@xiaomi.com
 */

package com.xiaomi.infra.galaxy.talos.mapreduce.input;

import org.apache.hadoop.mapreduce.Mapper;

import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicKeyWritable;
import com.xiaomi.infra.galaxy.talos.mapreduce.input.model.TalosTopicMessageWritable;

public abstract class TalosTopicMapper<KEYOUT, VALUEOUT> extends
    Mapper<TalosTopicKeyWritable, TalosTopicMessageWritable, KEYOUT, VALUEOUT> {
}
