package com.cycloneboy.bigdata.communication.converter;

import com.cycloneboy.bigdata.communication.kv.base.BaseDimension;

/** Create by sl on 2019-12-05 15:46 */
public interface DimensionConverter {

  int getDimensionId(BaseDimension dimension);
}
