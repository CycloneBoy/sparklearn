package com.cycloneboy.bigdata.communication.domain.dto.mafengwo;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Create by sl on 2019-08-11 15:06 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class TravelNoteDto {

  private Integer id;

  /** 游记年 */
  private Integer year;

  /** 游记月 */
  private Integer month;

  /** 游记日 */
  private Integer day;

  /** 游记链接 */
  private String url;

  /** 游记封面链接 */
  private String noteImageUrl;

  /** 游记目的地 */
  private String destination;

  /** 作者链接 */
  private String authorUrl;

  /** 作者名称 */
  private String authorName;

  /** 游记作者图片链接 */
  private String authorImageUrl;

  /** 创建时间 */
  private Date createTime;
}
