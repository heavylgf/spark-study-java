package cn.spark.study.structuredstreaming.kafka;

import jodd.util.PropertiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by CTWLPC on 2018/9/28.
 */
public class KafkaProducerTest {
    //static Producer<String,byte[]> kafkaProducer = new Producer<String, byte[]>(props);
    static KafkaProducer<String, String> producer = null;
    public void init()
    {
        Properties props = new Properties();
//        props.put("bootstrap.servers", PropertiesUtil.getValue("bootstrap.servers"));
        props.put("bootstrap.servers",
                "192.168.101.204:9092,192.168.101.205:9092,192.168.101.206:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void sendMsg(String topic,String msg) {


        producer.send(new ProducerRecord<String, String>(topic, msg, msg));

        //   producer.close();

    }

    //36 36 72
    public static void main(String[] args) {
        int hadSendTime = 0;
        //  while(true){
        int time = 10000;
        String reg_date = "";
        String breat_date = "";
        String photo_date = "";
        KafkaProducerTest test = new KafkaProducerTest();
        test.init();
        String x = UUID.randomUUID().toString().replaceAll("-", "");//ack ID //1505032861 1512489600
        for(int i = 0;i<time;i++) //0---5999 6000 time
        {
            //0---1999 2000 测试夸张一小时 和数据准确性
            //	int x=(int)(Math.random()*100000000);
            // int num =       a *    86400;                                                                                                                                                                                                                                        //这里是create时间
            reg_date = (1514736000 + 5000 + 86400 + 86400) + "";
            breat_date = (1514736000 + 5000 + 86400  + 86400) + "";
            photo_date = (1514736000 + 5000 + 86400  + 86400) + "";
//	    		if(i >= 50000) //2000---3999 2000
//	    		{
//	    			reg_date = (System.currentTimeMillis()/1000 + 86400) + "";
//	    			breat_date = (System.currentTimeMillis()/1000 + 86400) + "";
//	    		}

//	    		if(i >= 40) //2000---3999 2000
//	    		{
//	    			reg_date = (System.currentTimeMillis()/1000 + 86400 + 86400) + "";
//	    			breat_date = (System.currentTimeMillis()/1000 + 86400 + 86400) + "";
//	    		}

//	    		if(i >= 0)//4000---5999  2000
//	    		{
//	    			reg_date = (System.currentTimeMillis()/1000 + 86400 + 86400) + "";
//	    			breat_date = (System.currentTimeMillis()/1000 + 86400 + 86400) + "";
//	    		}
            //String tt = "sdkVersion_" + i + "|imei1_" + i + "|imei2_" + i + "|meid_" + i + "|deviceId_" + i + "|brand_" + i + "|model_" + i + "|firmwareVer_" + i + "|systemVer_" + i + "|type_" + i + "|iccid1_" + i + "|iccid2_" + i + "|imsi1_" + i + "|imsi2_" + i + "|mac_" + i + "|cellid_" + i + "|lac_" + i + "|channel_" + i + "|dataCard_" + i + "|masterStatus_" + i + "|volte_" + i + "|volteShow_" + i + "|noticecontent_" + i + "|volte2_" + i + "|volteShow2_" + i + "|noticecontent2_" + i + "|soltQuantity_" + i + "|dataCard2_" + i + "|soltService1_" + i + "|soltService2_" + i + "|soltNetwork1_" + i + "|soltNetwork2_" + i + "|lac2_" + i + "|cellId2_" + i + "|sendTime_" + i + "|" + breat_date + "|appKey_" + i + "|ip_" + i + "|totalFlowW_" + i + "|totalFlowD_" + i + "|intype_" + i + "|verify_" + i + "|cpu_" + i + "|rom_" + i + "|ram_" + i + "|totalFlowD2_" + i + "|mobileFirst_" + i + "|";
            String sendMSG_heartBeat = "2.0.32|861111131439427" + i + "|861111131439427|imei|861111131439427e8:99:c4:b1:86:55|aaa|htcmodel|ver|6.0.1|1|iccid|iccid|460222|460222|a1:b2:c3:d4:e5:f6|20|10|18|2|1|0|||0|||2|2|5|5|2|2|10|20|1502433983382|" +  breat_date  + "|A1234415234123|121.15.167.225|102400|204800|201606666||-2|-2|-2|204800|1";
            String sendMSG = "2.0.32|358202080042914|358202080042914|35820108004291|3582010800429163c:05:18:2a:05:19|samsung"+ i +"|SM-J3308|J3308ZMU0AQF1|7.1.1|MSM8937|32G|3GB|1|89860052011650104384|2|0e13d4588ad54e8fb96f68c7eb778a0b" + x + "|11118ad54e8fb96f68c7eb778a0b" + x + "|3C:05:18:2A:05:19|127206276|4288|18|0|0|2|0|5|5|13|0|2|2|1512489601|" + reg_date  + "|A100000004|101.96.133.242 |SM-J3308|imei_358201080042916#imsi_460000220269347#mac_3c:05:18:2a:05:19#brand_samsung#model_SM-J3308#version_7.1.1#totalRam_3GB#SDFreeSpace_21084.15625#cpu_Qualcomm Technologies, Inc MSM8917#screen_720*1280#simSerialNumber_89860052011650104384#romSpace_24.41 GB#battery_100#SIGNIFICANT_MOTION:N  STEP_COUNTER:N  ORIENTATION:N  TEMPERATURE:N  PROXIMITY:Y  ACCELEROMETER:Y  ROTATION_VECTOR:N  PRESSURE:N  GEOMAGNETIC_ROTATION_VECTOR:N  GYROSCOPE:N  LIGHT:N  MAGNETIC_FIELD_UNCALIBRATED:N  RELATIVE_HUMIDITY:N  GAME_ROTATION_VECTOR:N  AMBIENT_TEMPERATURE:N  GYROSCOPE_UNCALIBRATED:N  LINEAR_ACCELERATION:N  GRAVITY:N  STEP_DETECTOR:N  #|1";
            String sendMSG_photoAuth = "2.0.32|864444441439427|864444441439428|40000000000|86444444143942774:51:ba:ea:b0:a0|Xiaomi|MI 4LTE|ver|6.0.1|32核鬼畜CPU|8G|516MB|1|8600000000|8600000000|460001231231231|460001231231231|a1:b2:c3:d4:e5:f6|123|12345|18|1|0|2|1|5|5|13|13|12345|123|1516094997325|" + photo_date + "|A1234415234123|121.15.167.225|201606666||1|2.0.1|S000000001|1|8082a53ebe81-45d4-4928-b6f4-d792a242f6c6|1";
            //String sendMSG = "2.0.23|358202080042915|358202080042914|35820108004291|3582010800429163c:05:18:2a:05:19|samsung"+ i +"|SM-J3308|J3308ZMU0AQF1|7.1.1|MSM8937|32G|3GB|1|89860052011650104384|2|0e13d4588ad54e8fb96f68c7eb778a0b" + x + "|11118ad54e8fb96f68c7eb778a0b" + x + "|3C:05:18:2A:05:19|127206276|4288|18|0|0|2|0|5|5|13|0|2|2|1512489601|" + "1512835222" + "|A100000004|101.96.133.242 |SM-J3308|imei_358201080042916#imsi_460000220269347#mac_3c:05:18:2a:05:19#brand_samsung#model_SM-J3308#version_7.1.1#totalRam_3GB#SDFreeSpace_21084.15625#cpu_Qualcomm Technologies, Inc MSM8917#screen_720*1280#simSerialNumber_89860052011650104384#romSpace_24.41 GB#battery_100#SIGNIFICANT_MOTION:N  STEP_COUNTER:N  ORIENTATION:N  TEMPERATURE:N  PROXIMITY:Y  ACCELEROMETER:Y  ROTATION_VECTOR:N  PRESSURE:N  GEOMAGNETIC_ROTATION_VECTOR:N  GYROSCOPE:N  LIGHT:N  MAGNETIC_FIELD_UNCALIBRATED:N  RELATIVE_HUMIDITY:N  GAME_ROTATION_VECTOR:N  AMBIENT_TEMPERATURE:N  GYROSCOPE_UNCALIBRATED:N  LINEAR_ACCELERATION:N  GRAVITY:N  STEP_DETECTOR:N  #|1";
            //	System.out.println(sendMSG_heartBeat);
            test.sendMsg("photoAuth",sendMSG_photoAuth);
            test.sendMsg("heartbeat",sendMSG_heartBeat);
            test.sendMsg("reg",sendMSG);

            hadSendTime++;
            System.out.println("!!!" + hadSendTime);
        }


        producer.close();

    }

    // }
}
