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
            'type' => 'init',
            'data' => [
                'client_id' => $client_id
            ]

        ];
        Gateway::sendToClient($client_id, json_encode($data));
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
            $type = $message['type'];
            if ($type == 'location' && key_exists('locations', $message)) {
                $locations = $message['locations'];
                $u_id = Gateway::getUidByClientId($client_id);
                if (!$u_id) {
                    Gateway::sendToClient($client_id, json_encode([
                        'errorCode' => 1,
                        'msg' => '用户信息没有和websocket绑定，需要重新绑定'
                    ]));
                    return;
                }
                $arr = explode('-', $u_id);
                $u_id = $arr[1];
                $location_ids = self::prefixLocation($client_id, $u_id, $locations);
                Gateway::sendToClient($client_id, json_encode([
                    'errorCode' => 0,
                    'type' => 'uploadlocation',
                    'msg' => 'success',
                    'ids' => $location_ids
                ]));

            } else if ($type == "receivePush") {
                $p_id = $message['p_id'];
                self::receivePush($p_id);
            } else if ($type == "MINIPush") {
                $id = $message['id'];
                $u_id = $message['u_id'];
                self::MINIPush($id, $u_id);
            } else if ($type == 'checkOnline') {
                if (self::checkOnline($client_id)) {
                    Gateway::sendToClient($client_id, json_encode([
                        'errorCode' => 0,
                        'type' => 'checkOnline',
                        'msg' => 'success'
                    ]));
                } else {
                    Gateway::sendToClient($client_id, json_encode([
                        'errorCode' => 1,
                        'type' => 'checkOnline',
                        'msg' => 'fail'
                    ]));
                }

            }


        } catch (Exception $e) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 3,
                'msg' => $e->getMessage()
            ]));
            throw $e;
        }


    }

    private static function checkOnline($client_id)
    {
        $u_id = Gateway::getUidByClientId($client_id);
        if (!$u_id) {
            return 0;
        }
        $arr = explode('-', $u_id);
        $u_id = $arr[1];
        return $u_id;

    }

    private static function receivePush($p_id)
    {
        self::$db->update('drive_order_push_t')->cols(array('receive' => 1))->where('id=' . $p_id)->query();

    }

    private static function MINIPush($id, $u_id)
    {
        /*        self::$db->update('drive_mini_push_t')
                    ->cols(array('state' => 3))
                    ->where('id=' . $p_id)->query();*/
        self::$db->query("UPDATE `drive_mini_push_t` SET `state` = 3 WHERE o_id=" . $id . " AND u_id=" . $u_id);
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

        $location_ids = [];
        foreach ($locations as $k => $v) {
            array_push($location_ids, $v['locationId']);
            self::$db->insert('drive_location_t')->cols(
                array(
                    'lat' => $v['lat'],
                    'lng' => $v['lng'],
                    'citycode' => $v['citycode'],
                    'city' => $v['city'],
                    'district' => $v['district'],
                    'street' => $v['street'],
                    'addr' => $v['addr'],
                    'locationdescribe' => $v['locationdescribe'],
                    'phone_code' => $v['phone_code'],
                    'create_time' => $v['create_time'],
                    'update_time' => $v['create_time'],
                    'location_id' => $v['locationId'],
                    'loc_type' => $v['locType'],
                    'o_id' => key_exists('o_id', $v) ? $v['o_id'] : '',
                    'begin' => key_exists('begin', $v) ? $v['begin'] : 2,
                    'u_id' => $u_id
                )
            )->query();
            $driver = self::$db->select('id,username,phone')->from('drive_driver_t')
                ->where('id= :id')->bindValues(array('id' => $u_id))->row();

            if ($k == 0) {
                //将地理位置存储到redis,并更新行动距离
                //1.先删除旧的实时地理位置
                self::$redis->rawCommand('zrem', 'drivers_tongling', $u_id);
                //2.新增新的实时地理位置
                $ret = self::$redis->rawCommand('geoadd', 'drivers_tongling', $v['lng'], $v['lat'], $u_id);
                //3.保存司机位置名称信息
                $location_data = [
                    'username' => $driver ? $driver['username'] : '',
                    'phone' => $driver ? $driver['phone'] : '',
                    'lat' => $v['lat'],
                    'lng' => $v['lng'],
                    'citycode' => $v['citycode'],
                    'city' => $v['city'],
                    'district' => $v['district'],
                    'street' => $v['street'],
                    'addr' => $v['addr'],
                    'locationdescribe' => $v['locationdescribe']
                ];

                self::$redis->rPush("driver:$u_id:location", json_encode($location_data));
                if (!$ret) {
                    Gateway::sendToClient($client_id, json_encode([
                        'errorCode' => 7,
                        'msg' => '写入redis失败'
                    ]));
                    //return;
                }
            }

            if (key_exists('o_id', $v) && strlen($v['o_id'])
                && key_exists('begin', $v) && $v['begin' == 1]
            ) {
                self::prefixDistance($v['o_id'], $old_lng, $old_lat, $v['lng'], $v['lat']);
            }

            $old_lng = $v['lng'];
            $old_lat = $v['lng'];

        }
        return implode(',', $location_ids);

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
        $u_id = Gateway::getUidByClientId($client_id);
        self::$db->insert('drive_socket_closed_t')->cols(
            array(
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
                'u_id' => $u_id
            )
        )->query();
    }
}
