package com.cycloneboy.bigdata.hbaselearn.weibo;

import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

/** Create by sl on 2019-12-03 12:47 */
@Slf4j
public class WeiboUtilsTest {

  WeiboUtils weiboUtils = null;

  String uid_1001 = "1001";
  String uid_1002 = "1002";
  String uid_1003 = "1003";

  @Before
  public void setUp() throws Exception {
    weiboUtils = new WeiboUtils();
  }

  @Test
  public void initNamespace() {
    weiboUtils.initNamespace();
    log.info("初始化命名空间");
  }

  @Test
  public void creatTable() {
    weiboUtils.createTableContent();
    weiboUtils.createTableRelation();
    weiboUtils.createTableInbox();
    log.info("初始表格");
  }

  @Test
  public void publishContent() {

    weiboUtils.publishContent(uid_1001, "今天不上班");
    weiboUtils.publishContent(uid_1001, "今天天气好");
    weiboUtils.publishContent(uid_1001, "今天吃的好饱");
    weiboUtils.publishContent(uid_1001, "明天去哪里玩");
    weiboUtils.publishContent(uid_1001, "还有一个多月过年");
    weiboUtils.publishContent(uid_1001, "听说你回湖北了");

    List<WeiBoMessage> messages1 = weiboUtils.getUserContent(uid_1001);

    messages1.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));

    weiboUtils.publishContent(uid_1002, "写一点代码吧");
    weiboUtils.publishContent(uid_1002, "出去运动会");
    weiboUtils.publishContent(uid_1002, "跑个步怎么样");
    weiboUtils.publishContent(uid_1002, "我们什么时候取爬山");
    weiboUtils.publishContent(uid_1002, "圣诞节想干嘛");
    weiboUtils.publishContent(uid_1002, "走,出去吃饭去");

    List<WeiBoMessage> messages2 = weiboUtils.getUserContent(uid_1002);

    messages2.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));

    weiboUtils.publishContent(uid_1003, "小学生在学校上课");
    weiboUtils.publishContent(uid_1003, "初中生在学校背书");
    weiboUtils.publishContent(uid_1003, "高中生在学校打球");
    weiboUtils.publishContent(uid_1003, "大学生在学校打游戏看剧");
    weiboUtils.publishContent(uid_1003, "研究生在学校做项目");
    weiboUtils.publishContent(uid_1003, "博士生在学校写论文");

    List<WeiBoMessage> messages3 = weiboUtils.getUserContent(uid_1003);

    messages3.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));
  }

  @Test
  public void addAttends() {
    weiboUtils.addAttends(uid_1001, Arrays.asList(uid_1002, uid_1003));

    List<WeiBoMessage> message = weiboUtils.getAttendsContent(uid_1001);
    message.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));
  }

  @Test
  public void removeAttends() {
    weiboUtils.removeAttends(uid_1001, Arrays.asList(uid_1002));

    List<WeiBoMessage> message = weiboUtils.getAttendsContent(uid_1001);
    message.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));
  }

  @Test
  public void getAttendsContent() {

    weiboUtils.removeAttends(uid_1001, Arrays.asList(uid_1003));
    List<WeiBoMessage> message = weiboUtils.getAttendsContent(uid_1001);
    message.forEach(weiBoMessage -> log.info(weiBoMessage.toString()));
  }
}
