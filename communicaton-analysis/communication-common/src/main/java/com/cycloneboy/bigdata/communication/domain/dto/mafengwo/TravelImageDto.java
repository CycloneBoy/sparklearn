package com.cycloneboy.bigdata.communication.domain.dto.mafengwo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cycloneboy
 * @since 2019-03-31
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class TravelImageDto {

  private String id;

  private String noteId;

  private String imageId;

  private Integer voteNum;

  private Integer replyNum;

  private Integer shareNum;

  private String originalUrl;

  /** 视频名称 */
  private String name;

  /** 视频分类 */
  private String category;

  /** 视频封面 */
  private String cover;

  /** 视频链接 */
  private String url;
}
