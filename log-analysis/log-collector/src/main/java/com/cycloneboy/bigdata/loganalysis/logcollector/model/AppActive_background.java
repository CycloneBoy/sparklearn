package com.cycloneboy.bigdata.loganalysis.logcollector.model;

import lombok.Data;

/** Create by sl on 2020-03-16 21:18 */
@Data
public class AppActive_background {

  private String active_source; // 1=upgrade,2=download(下载),3=plugin_upgrade
}
