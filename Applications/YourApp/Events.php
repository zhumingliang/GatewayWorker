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

    /**
     * 进程启动后初始化数据库连接
     */
    public static function onWorkerStart($worker)
    {
        self::$db = new \Workerman\MySQL\Connection('55a32a9887e03.gz.cdb.myqcloud.com',
            '16273', 'cdb_outerroot', 'Libo1234', 'drive');
    }


    /**
     * 当客户端连接时触发
     * 如果业务不需此回调可以删除onConnect
     *
     * @param int $client_id 连接id
     */
    public static function onConnect($client_id)
    {
        Gateway::sendToClient($client_id, json_encode(array(
            'type' => 'init',
            'client_id' => $client_id
        )));

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

        $u_id = Gateway::getUidByClientId($client_id);
        if (!$u_id) {
            Gateway::sendToClient($client_id, json_encode([
                'errorCode' => 1,
                'msg' => '用户信息没有和websocket绑定，需要重新绑定'
            ]));
            return;
        }

        $message = json_decode($message, true);
        if (!key_exists('type', $message)) {
            $return_data = [
                'errorCode' => 2,
                'msg' => '非法请求'
            ];
            Gateway::sendToClient($client_id, json_encode($return_data));
            return;
        }

        $type = $message['type'];
        if ($type == 'location' && key_exists('locations', $message)) {
            $locations = $message['locations'];
            if (!count($locations)) {
                Gateway::sendToClient($client_id, json_encode([
                    'errorCode' => 3,
                    'msg' => '地理位置信息不能为空'
                ]));
                return;
            }

            foreach ($locations as $k => $v) {
                self::$db->insert('drive_location_t')->cols(
                    array(
                        'lat' => $v['lat'],
                        'lng' => $v['lng'],
                        'create_time' => date("Y-m-d H:i:s", time()),
                        'update_time' => date("Y-m-d H:i:s", time()),
                        'o_id' => key_exists('o_id') ? $v['o_id'] : '',
                        'u_id' => $u_id
                    )
                )->query();

                Gateway::sendToClient($client_id, json_encode([
                    'errorCode' => 0,
                    'msg' => 'success'
                ]));
                return;
            }


        }


    }

    /**
     * 当用户断开连接时触发
     * @param int $client_id 连接id
     */
    public static function onClose($client_id)
    {
        // 向所有人发送
        GateWay::sendToAll("$client_id logout\r\n");
    }
}
