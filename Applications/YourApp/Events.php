<?php
/**
 * This file is part of workerman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the MIT-LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @author walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link http://www.workerman.net/
 * @license http://www.opensource.org/licenses/mit-license.php MIT License
 */

/**
 * 用于检测业务代码死循环或者长时间阻塞等问题
 * 如果发现业务卡死，可以将下面declare打开（去掉//注释），并执行php start.php reload
 * 然后观察一段时间workerman.log看是否有process_timeout异常
 */

//declare(ticks=1);

use \GatewayWorker\Lib\Gateway;

/**
 * 主逻辑
 * 主要是处理 onConnect onMessage onClose 三个方法
 * onConnect 和 onClose 如果不需要可以不用实现并删除
 */
class Events
{


    /**
     * 新建一个类的静态成员，用来保存数据库实例
     */
    public static $db = null;
    public static $redis = null;

    /**
     * 进程启动后初始化数据库连接
     */
    public static function onWorkerStart($worker)
    {
        self::$db = new \Workerman\MySQL\Connection('55a32a9887e03.gz.cdb.myqcloud.com',
            '16273', 'cdb_outerroot', 'Libo1234', 'drive');


        self::$redis = new Redis();
        self::$redis->connect('127.0.0.1', 6379, 60);
    }


    /**
     * 当客户端连接时触发
     * 如果业务不需此回调可以删除onConnect
     *
     * @param int $client_id 连接id
     */
    public static function onConnect($client_id)
    {

        $data = [
            'errorCode' => 0,
            'msg' => 'success',
            'data' => [
                'type' => 'init',
                'client_id' => $client_id
            ]

        ];
        Gateway::sendToClient($client_id, json_encode($data));

        /*  // 向当前client_id发送数据
          Gateway::sendToClient($client_id, "Hello $client_id\r\n");
          // 向所有人发送
          Gateway::sendToAll("$client_id login\r\n");*/
    }

    /**
     * 当客户端发来消息时触发
     * @param int $client_id 连接id
     * @param mixed $message 具体消息
     */
    public static function onMessage($client_id, $message)
    {

        try {

            $message = json_decode($message, true);
            if (!key_exists('type', $message)) {
                return;
            }
            $u_id = Gateway::getUidByClientId($client_id);
            if (!$u_id) {
                Gateway::sendToClient($client_id, json_encode([
                    'errorCode' => 1,
                    'msg' => '用户信息没有和websocket绑定，需要重新绑定'
                ]));
                return;
            }
            $type = $message['type'];
            if ($type == 'location' && key_exists('locations', $message)) {
                $locations = $message['locations'];
                self::prefixLocation($client_id, $u_id, $locations);
            }


        } catch (Exception $e) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 3,
                'msg' => $e->getMessage()
            ]));
        }


    }

    private static function prefixLocation($client_id, $u_id, $locations)
    {


        if (!count($locations)) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 3,
                'msg' => '地理位置信息不能为空'
            ]));
            return;
        }
        $old_lng = 0;
        $old_lat = 0;
        $old_location = self::$redis->rawCommand('geopos', 'drivers_tongling', $u_id);
        if ($old_location) {
            $old_lng = $old_location[0][0];
            $old_lat = $old_location[0][1];
        }

        foreach ($locations as $k => $v) {
            self::$db->insert('drive_location_t')->cols(
                array(
                    'lat' => $v['lat'],
                    'lng' => $v['lng'],
                    'phone_code' => $v['phone_code'],
                    'create_time' => $v['create_time'],
                    'update_time' => $v['create_time'],
                    'o_id' => key_exists('o_id', $v) ? $v['o_id'] : '',
                    'begin' => key_exists('begin', $v) ? $v['begin'] : 2,
                    'u_id' => $u_id
                )
            )->query();
            if ($k == 0) {
                //将地理位置存储到redis,并更新行动距离
                //1.先删除旧的实时地理位置
                self::$redis->rawCommand('zrem', 'drivers_tongling', $u_id);
                //2.新增新的实时地理位子
                $ret = self::$redis->rawCommand('geoadd', 'drivers_tongling', $v['lng'], $v['lat'], $u_id);
                if (!$ret) {
                    Gateway::sendToClient($client_id, json_encode([
                        'errorCode' => 7,
                        'msg' => '写入redis失败'
                    ]));
                    //return;
                }
            }

            if (key_exists('o_id', $v) && strlen($v['o_id'])
                && key_exists('begin', $v) && $v['begin' == 1]) {
                self::prefixDistance($v['o_id'], $old_lng, $old_lat, $v['lng'], $v['lat']);
            }

            $old_lng = $v['lng'];
            $old_lat = $v['lng'];

        }


    }

    private static function prefixDistance($o_id, $old_lng, $old_lat, $new_lng, $new_lat)
    {
        $order_id = 'o:' . $o_id;
        //获取距离并增加新距离
        $distance = self::$redis->zScore('order:distance', $order_id);
        if ($distance === false) {
            $res = self::$redis->zAdd('order:distance', 0, $order_id);
            return $res;
        }
        if ($old_lng == 0 || $old_lat == 0 || $new_lng == 0 || $new_lat == 0) {
            $dis = 0;
        } else {
            $dis = self::GetDistance($old_lng, $old_lat, $new_lng, $new_lat);
        }
        $res = self::$redis->zIncrBy('order:distance', $dis, $order_id);
        return $res;
    }

    public static function GetDistance($lat1, $lng1, $lat2, $lng2)
    {
        $radLat1 = $lat1 * pi() / 180.0;
        $radLat2 = $lat2 * pi() / 180.0;
        $a = $radLat1 - $radLat2;
        $b = $lng1 * pi() / 180.0 - $lng2 * pi() / 180.0;
        $s = 2 * asin(sqrt(pow(sin($a / 2), 2) +
                cos($radLat1) * cos($radLat2) * pow(sin($b / 2), 2)));
        $s = $s * 6378.137;
        $s = round($s * 10000) / 10000;
        return $s;
    }

    /**
     * 当用户断开连接时触发
     * @param int $client_id 连接id
     */
    public static function onClose($client_id)
    {
        // 向所有人发送
        //GateWay::sendToAll("$client_id logout\r\n");
    }
}
