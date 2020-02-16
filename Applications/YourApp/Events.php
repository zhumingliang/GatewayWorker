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
use think\Db;

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
    public static $http = null;

    public static function orderHandel()
    {
        //order:no;order:ing;order:complete
        //1.未处理集合随机取出一个元素
        $order_id = self::$redis->sPop('order:no');
        if (!$order_id) {
            return true;
        }
        //2.将元素添加至正在处理集合
        self::$redis->sAdd('order:ing', $order_id);

        //3.获取订单
        $order = self::$db->select('*')->from('drive_order_t')
            ->where('id= :order_id')->bindValues(array('order_id' => $order_id))->row();
        if (!$order || $order['state'] != 1) {
            //订单不存在或者状态不是未接单
            self::$redis->sRem('order:ing', $order_id);
            self::$redis->sAdd('order:complete', $order_id);
            return true;
        }
        //4.查询可派送司机
        $res = self::findDriverToPush($order);
        if (!$res) {
            self::$redis->sRem('order:ing', $order_id);
            self::$redis->sAdd('order:no', $order_id);
        }
    }

    private static function sendMsg($params)
    {
        $rule = "https://tonglingok.com/api/v1/sms/driver";
        self::$http->post($rule, $params, function ($response) {
            self::saveLog("发送短信成功:" . $response->getBody());
        }, function ($exception) {
            self::saveLog("发送短信失败：" . $exception);

        });
    }

    private static function saveLog($msg)
    {
        self::$db->insert('drive_log_t')->cols(
            array(
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
                'msg' => $msg
            )
        )->query();
    }

    private static function findDriverToPush($order)
    {

        $company_id = $order['company_id'];
        //查询所有司机并按距离排序
        $lat = $order['start_lat'];
        $lng = $order['start_lng'];
        $driver_location_key = "driver:location:$company_id";
        $list = self::$redis->rawCommand('georadius',
            $driver_location_key, $lng, $lat,
            20,
            'km', 'ASC');
        if (!count($list)) {
            return false;
        }
        $push = false;
        //设置三个set: 司机未接单 driver_order_no；司机正在派单 driver_order_ing；司机已经接单 driver_order_receive
        foreach ($list as $k => $v) {
            $d_id = $v;
            $checkDriver = self::checkDriverCanReceiveOrder($d_id);
            if ($checkDriver) {
                $check = self::checkDriverPush($order->id, $d_id);
                if ($check == 2) {
                    continue;
                }

                //将司机从'未接单'移除，添加到：正在派单
                self::$redis->sRem('driver_order_no:' . $company_id, $d_id);
                self::$redis->sRem('driver_order_receive:' . $company_id, $d_id);
                self::$redis->sAdd('driver_order_ing:' . $company_id, $d_id);

                //通过短信推送给司机
                $phone = self::$redis->hGet('driver:' . $d_id, 'phone');

                $send_data = [
                    'phone' => $phone, 'order_num' => $order['order_num'],
                    'create_time' => $order['create_time']
                ];
                self::sendMsg($send_data);
                if ($order['from'] == "小程序下单" && $order['company_id'] == 1) {
                    $send_data = [
                        'phone' => '13515623335', 'order_num' => $order['order_num'],
                        'create_time' => $order['create_time']
                    ];
                    self::sendMsg($send_data);
                }
                $push_id = self::$db->insert('drive_order_push_t')->cols(
                    [
                        'd_id' => $d_id,
                        'o_id' => $order['id'],
                        'type' => 'normal',
                        'state' => 1,
                        'create_time' => date('Y-m-d H:i:s'),
                        'update_time' => date('Y-m-d H:i:s'),
                        'limit_time' => time()
                    ]
                )->query();
                $driver_location = self::getDriverLocation($d_id, $company_id);
                //通过websocket推送给司机
                $push_data = [
                    'type' => 'order',
                    'order_info' => [
                        'o_id' => $order['id'],
                        'from' => "系统派单",
                        'name' => $order['name'],
                        'phone' => $order['phone'],
                        'start' => $order['start'],
                        'end' => $order['end'],
                        'distance' => CalculateUtil::GetDistance($lat, $lng, $driver_location['lat'], $driver_location['lng']),
                        'create_time' => $order['create_time'],
                        'p_id' => $push_id

                    ]
                ];
                Gateway::sendToUid('driver' . '-' . $d_id, self::prefixMessage($push_data));
                self::$db->update('drive_order_push_t')
                    ->cols(array('message' => json_encode($push_data)))
                    ->where('id=' . $push_id)->query();
                $push = true;
                break;
            }

        }
        return $push;
    }

    private static function prefixMessage($message)
    {
        $data = [
            'errorCode' => 0,
            'msg' => 'success',
            'type' => $message['type'],
            'data' => $message['order_info']

        ];
        return json_encode($data);

    }

    public static function getDriverLocation($u_id, $company_id = '')
    {

        $company_id = empty($company_id) ? 1 : $company_id;
        $driver_location_key = "driver:location:$company_id";
        $location = self::$redis->rawCommand('geopos', $driver_location_key, $u_id);
        if ($location) {
            $lng = empty($location[0][0]) ? null : $location[0][0];
            $lat = empty($location[0][1]) ? null : $location[0][1];
        } else {
            $lng = null;
            $lat = null;
        }

        return [
            'lng' => $lng,
            'lat' => $lat,
        ];
    }

    public static function checkDriverCanReceiveOrder($d_id)
    {
        if (!Gateway::isUidOnline('driver' . '-' . $d_id)) {
            return false;
        }
        $company_id = self::$redis->hGet('driver:' . $d_id, 'company_id');
        $company_id = empty($company_id) ? 1 : $company_id;

        if (!(self::$redis->sIsMember('driver_order_no:' . $company_id, $d_id))) {
            return false;
        }
        return self::checkDriverOnline($d_id);
    }

    private static function checkDriverOnline($d_id)
    {
        $driver = self::$db->select('online')
            ->from('drive_driver_t')
            ->where('id= :driver_id')
            ->bindValues(array('driver_id' => $d_id))->row();

        if ($driver['online'] == 1) {
            return true;
        }
        return false;
    }

    private static function checkDriverPush($o_id, $d_id)
    {
        $pushes = self::$db->select('*')
            ->from('drive_order_push_t')
            ->where('o_id= :order_id AND d_id= :driver_id AND receive= :receive_state')
            ->bindValues(array('order_id' => $o_id, 'driver_id' => $d_id, 'receive_state' => 1))
            ->query();

        self::saveLog("dp" . json_encode($pushes));
        if (!count($pushes)) {
            return 1;
        }
        foreach ($pushes as $k => $v) {
            if ($v['state'] == 3) {
                return 2;
                break;
            }
        }
        if (count($pushes) >= 3) {
            return 2;
        }
        return 1;
    }

    public static function handelMiniNoAnswer()
    {
        $push = self::$db->query("SELECT * FROM `drive_mini_push_t` WHERE state <> 3 AND count < 10");

        if (count($push)) {
            foreach ($push as $k => $v) {
                $online = false;
                $u_id = $v['u_id'];
                $message = json_decode($v['message'], true);
                if ($v['send_to'] == 1) {
                    if (Gateway::isUidOnline('mini' . '-' . $u_id)) {
                        $online = true;
                        Gateway::sendToUid('mini' . '-' . $u_id, self::prefixMessage($message));
                    }
                } else if ($v['send_to'] == 2) {
                    if (Gateway::isUidOnline('driver' . '-' . $u_id)) {
                        $online = true;
                        Gateway::sendToUid('driver' . '-' . $u_id, self::prefixMessage($message));
                    }
                }

                if ($online) {
                    self::$db->update('drive_mini_push_t')->cols(array('count' => $v['count'] + 1))->where('id=' . $v['id'])->query();
                }
            }
        }


    }

    public static function handelDriverNoAnswer()
    {
        $push = self::$db->select('*')
            ->from('drive_order_push_t')
            ->where('state= :order_state')
            ->bindValues(array('order_state' => 1))
            ->query();
        self::saveLog(json_encode($push));

        if (count($push)) {
            foreach ($push as $k => $v) {
                $d_id = $v['d_id'];
                $order_id = $v['o_id'];
                $company_id = self::$redis->hGet('driver:' . $d_id, 'company_id');
                if (time() > $v['limit_time'] + 45) {
                    self::$redis->sRem('driver_order_receive:' . $company_id, $d_id);
                    self::$redis->sRem('driver_order_ing:' . $company_id, $d_id);
                    self::$redis->sAdd('driver_order_no:' . $company_id, $d_id);

                    //将订单由正在处理集合改为未处理集合
                    self::$redis->sRem('order:ing', $order_id);
                    self::$redis->sAdd('order:no', $order_id);

                    self::$db->update('drive_order_push_t')->cols(array('state' => 4))->where('id=' . $v['id'])->query();
                } else {

                    if ($v['receive'] == 2 && !empty($v['message'])
                        && Gateway::isUidOnline('driver' . '-' . $d_id)
                        && (self::checkDriverOnline($d_id))) {
                        Gateway::sendToUid('driver' . '-' . $d_id, self::prefixMessage(json_decode($v['message'], true)));
                    }
                }
            }


        }
    }


    /**
     * 进程启动后初始化数据库连接
     */
    public
    static function onWorkerStart($worker)
    {
        self::$db = new \Workerman\MySQL\Connection('55a32a9887e03.gz.cdb.myqcloud.com',
            '16273', 'cdb_outerroot', 'Libo1234', 'drive');

        self::$redis = new Redis();
        self::$redis->connect('127.0.0.1', 6379, 60);
        self::$http = new Workerman\Http\Client();

        \Workerman\Lib\Timer::add(1, function () use ($worker) {
            self::handelDriverNoAnswer();
            self::handelMiniNoAnswer();
            self::orderHandel();
        });
    }


    /**
     * 当客户端连接时触发
     * 如果业务不需此回调可以删除onConnect
     *
     * @param int $client_id 连接id
     */
    public
    static function onConnect($client_id)
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
    public
    static function onMessage($client_id, $message)
    {

        try {
            $message = json_decode($message, true);
            if (!key_exists('type', $message)) {
                return;
            }
            $type = $message['type'];
            if ($type == 'location' && key_exists('locations', $message)) {
                $locations = $message['locations'];
                $current = array();
                if (!empty($message['current'])) {
                    $current = $message['current'];
                }
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
                $version = 1;
                if (!empty($message['version'])) {
                    $version = $message['version'];
                }
                $location_ids = self::prefixLocation($version, $client_id, $u_id, $locations, $current);
                Gateway::sendToClient($client_id, json_encode([
                    'errorCode' => 0,
                    'type' => 'uploadlocation',
                    'msg' => 'success',
                    'data' => $location_ids
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

            } else if ($type == 'canteenConsumption') {
                self::canteenConsumption($client_id);

            }

        } catch (Exception $e) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 3,
                'msg' => $e->getMessage()
            ]));
            throw $e;
        }


    }

    private
    static function checkOnline($client_id)
    {
        $u_id = Gateway::getUidByClientId($client_id);
        if (!$u_id) {
            return 0;
        }
        $arr = explode('-', $u_id);
        $u_id = $arr[1];
        return $u_id;

    }

    private
    static function receivePush($p_id)
    {
        self::$db->update('drive_order_push_t')->cols(array('receive' => 1))->where('id=' . $p_id)->query();

    }

    private
    static function MINIPush($id, $u_id)
    {
        self::$db->query("UPDATE `drive_mini_push_t` SET `state` = 3 WHERE o_id=" . $id . " AND u_id=" . $u_id);
    }

    private
    static function prefixLocation($version, $client_id, $u_id, $locations, $current)
    {

        $current_save = false;
        if (!empty($current) && !empty($current['lat']) && !empty($current['lng'])) {
            if ($version == 1) {
                self::saveDriverCurrentLocationV2($client_id, $current['lat'], $current['lng'], $u_id);
            } else {
                self::saveDriverCurrentLocationV2($client_id, $current['lat'], $current['lng'], $u_id);
            }
            $current_save = true;
        }
        if (!count($locations)) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 3,
                'msg' => '地理位置信息不能为空'
            ]));
            return;
        }

        $location_ids = [];
        foreach ($locations as $k => $v) {
            array_push($location_ids, $v['locationId']);
            if (!$current_save && $k == 0) {
                if ($version == 1) {
                    self::saveDriverCurrentLocationV2($client_id, $v['lat'], $v['lng'], $u_id);

                } else {
                    self::saveDriverCurrentLocationV2($client_id, $v['lat'], $v['lng'], $u_id);
                }
            }
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
                    'up_time' => date('Y-m-d H:i:s'),
                    'baidu_time' => $v['createTime'],
                    'location_id' => $v['locationId'],
                    'loc_type' => $v['locType'],
                    'o_id' => key_exists('o_id', $v) ? $v['o_id'] : '',
                    'begin' => key_exists('begin', $v) ? $v['begin'] : 2,
                    'u_id' => $u_id
                )
            )->query();

        }
        return implode(',', $location_ids);

    }

    private
    static function saveDriverCurrentLocationV2($client_id, $lat, $lng, $u_id)
    {
        //获取司机信息
        $driver = self::getDriverInfo($u_id);
        $company_id = empty($driver['company_id']) ? 1 : $driver['company_id'];
        $save_location_key = "driver:location:$company_id";
        //将地理位置存储到redis,并更新行动距离
        //1.先删除旧的实时地理位置(driver:company_id:location)
        self::$redis->rawCommand('zrem', $save_location_key, $u_id);
        //2.新增新的实时地理位置
        $ret = self::$redis->rawCommand('geoadd', $save_location_key, $lng, $lat, $u_id);
        if (!$ret) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 7,
                'msg' => '写入redis失败'
            ]));
        }
    }

    private
    static function getDriverInfo($u_id)
    {
        $driver_id = 'driver:' . $u_id;
        $driver = self::$redis->hMGet($driver_id, ['company_id', 'phone', 'username']);
        if (empty($driver)) {
            $driver = self::$db->select('id,username,number,company_id')
                ->from('drive_driver_t')
                ->where('id= :id')
                ->bindValues(array('id' => $u_id))
                ->row();
        }
        return $driver;
    }


    /**
     * 当用户断开连接时触发
     * @param int $client_id 连接id
     */
    public
    static function onClose($client_id)
    {
        // 向所有人发送
        //GateWay::sendToAll("$client_id logout\r\n");
        /* self::$db->insert('drive_socket_closed_t')->cols(
             array(
                 'create_time' => date('Y-m-d H:i:s'),
                 'update_time' => date('Y-m-d H:i:s'),
                 'client_id' => $client_id,
                 'u_id' => self::checkOnline($client_id)
             )
         )->query();*/
    }
}
