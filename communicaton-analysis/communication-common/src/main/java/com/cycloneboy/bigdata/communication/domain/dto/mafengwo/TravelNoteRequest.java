package com.cycloneboy.bigdata.communication.domain.dto.mafengwo;

import com.cycloneboy.bigdata.communication.domain.PageQueryRequest;
import lombok.Data;

/** Create by sl on 2019-08-11 13:33 */
@Data
public class TravelNoteRequest extends PageQueryRequest {

  private Integer year;

  private Integer month;

  private Integer day;

  private String destination;

  private String authorName;
}
