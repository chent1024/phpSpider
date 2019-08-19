<?php


namespace PhpSpider;

use GuzzleHttp\Pool;
use GuzzleHttp\Client as HttpClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

class Spider
{
    /**
     * 爬虫配置
     *
     * @var array
     */
    private $config = [];

    /**
     * 日志类
     *
     * @var Logger
     */
    protected $log;

    /**
     * redis 连接
     *
     * @var \Predis\Client
     */
    protected $redis;

    /**
     * redis 前缀
     *
     * @var string
     */
    protected $redisPrefix = 'php-spider.';

    /**
     * 采集状态
     *
     * @var array
     */
    protected $status = [
        'success_count' => 0,
        'request_error_pages' => 0,
        'save_error_pages' => 0,
    ];

    public function __construct(array $config = [])
    {
        $this->setConfig($config);
        $this->setRedisPrefix();
        $this->initLog();
        $this->setRedis();
    }

    /**
     * set config
     *
     * @param $config
     * @throws \Exception
     */
    protected function setConfig($config)
    {
        $defConfig = [
            'name' => NULL, // 爬虫名称
            'concurrency' => 1, // 并发线程数
            'continue' => 0, // 是否开启续爬
            'timeout' => 10.0,    // 爬取网页超时时间
            'log_step' => 2, // 每爬取多少页面记录一次日志
            'base_uri' => '', // 爬取根域名
            'interval' => 0, // 每次爬取间隔时间
            'queue_len' => NULL, // 队列长度，用于记录队列进度日志
            'retry_count' => 2, // 失败重试次数
            'check_black' => 1, // 是否判断黑名单
            'requests' => function () { // 需要发送的请求
            },
            'success' => function ($result, $request, $spider, $headers) { // 爬取成功的回调函数
            },
            'error' => function ($request, $error, $result) { // 爬取失败的回调函数
            },
        ];

        // 判断name未赋值，则抛出异常
        if (empty($config['name'])) {
            throw new \Exception('爬虫name未定义');
        }

        // 合并数组
        $this->config = array_merge($defConfig, $config);
    }

    /**
     * 爬虫redis数据前缀
     *
     * @return void
     */
    protected function setRedisPrefix()
    {
        $this->redisPrefix .= $this->config['name'];
    }

    /**
     * 初始化log[Monolog]
     *
     * @throws \Exception
     */
    protected function initLog()
    {
        $this->log = new Logger($this->redisPrefix);
        $this->log->pushHandler(new StreamHandler(Utils::config('log.path'), Logger::INFO));
    }

    /**
     * set redis
     *
     * @return void
     */
    protected function setRedis()
    {
        $this->redis = Utils::redis();
    }

    /**
     * 执行并发爬取
     *
     * @return void
     */
    public function run()
    {
        $startRunTime = microtime(TRUE);

        // 开启断点续爬且有未完成的数据，则续爬
        if ($this->config['continue'] && $this->getRequestOverplus()) {
            $this->pushContinueRequestToRedis();
        } else {
            $this->pushRequestToRedis();
        }

        // 进行数据爬取
        $this->doRequest();

        $this->setEndLogs($startRunTime);
    }

    /**
     * 记录日志
     *
     * @param $startRunTime
     * @return array
     */
    protected function setEndLogs($startRunTime)
    {
        $takeTime = number_format((microtime(TRUE) - $startRunTime), 6);
        $loginInfo[] = "耗时:" . $takeTime . 's';
        $loginInfo[] = "线程数:" . $this->config['concurrency'];
        $loginInfo[] = "请求数:" . $this->getRequestTotal();
        $loginInfo[] = "请求成功:" . $this->status['success_count'];
        $loginInfo[] = "请求失败:" . $this->status['request_error_pages'];
        $loginInfo[] = "解析失败:" . $this->status['save_error_pages'];
        $this->log->info('爬取 end', $loginInfo);

        return $loginInfo;
    }

    /**
     * 进行采集请求
     *
     * @return void
     */
    protected function doRequest()
    {
        $this->log->info('爬取 start');

        // 实例化guzzle
        $client = new HttpClient([
            'timeout' => $this->config['timeout'],
        ]);
        while ($this->getRequestOverplus()) {
            // 获取请求闭包函数
            $requests = function () use ($client) {
                $i = 0;

                while (($request = $this->rpopErrorRequest()) || ($request = $this->rpopRequest())) {
                    $this->hsetRequesting($i, $request);
                    $request = $this->getRealRequest($request);
                    yield function () use ($client, $request) {
                        $options = $request;
                        return $client->requestAsync($request['method'], $request['uri'], $options);
                    };

                    $i++;
                }
            };

            // 爬取网站数据
            $pool = new Pool($client, $requests(), [
                'concurrency' => $this->config['concurrency'],
                'fulfilled' => function ($response, $index) {
                    $this->status['success_count']++;

                    $this->decrRequestOverplus();
                    $this->hdelRequesting($index);

                    $request = $this->hgetRequesting($index);
                    $this->redis->hdel($this->redisKey('retry_count'), $request); // 删除该请求失败重试次数

                    // 执行请求成功回调函数
                    $result = $response->getBody()->getContents();
                    $request = json_decode($request, true);
                    try {
                        $callbackSuccess = $this->config['success']($result, $request, $this, $response->getHeaders());

                        // 错误日志
                        if (isset($callbackSuccess['status']) && $callbackSuccess['status'] <= 0) {
                            sort($callbackSuccess['error_resaons']);
                            $this->addRequestErrorLog($request, 'save_validate', $callbackSuccess['error_resaons']);
                        }
                    } catch (\Exception $e) {
                        $errorMsg = $e->getMessage() . ' in ' . $e->getFile() . ' on ' . $e->getLine();
                        $this->addRequestErrorLog($request, 'crawler_exception', $errorMsg);
                        $this->status['save_error_pages']++;
                    }

                    $this->addRequestProcessLog();

                    // 爬取sleep间隔
                    $this->config['interval'] && sleep($this->config['interval']);
                },
                'rejected' => function ($reason, $index) {
                    $this->hdelRequesting($index);
                    $this->status['request_error_pages']++;

                    // 获取请求数据
                    $request = $this->hgetRequesting($index);
                    $this->addRequestErrorLog($request, 'request_fail', $reason->getMessage());

                    // 判断失败次数超过限制，则跳过该请求
                    $retryCount = $this->redis->hget($this->redisKey('retry_count'), $request);
                    if ($retryCount >= $this->config['retry_count']) {
                        // 调用爬取失败回调函数
                        $result = $reason->getResponse() ? $reason->getResponse()->getBody()->getContents() : null;
                        $this->config['error'](json_decode($request, true), $reason->getMessage(), $result);

                        $this->addRequestErrorLog(json_decode($request, true), 'request_fail', $reason->getMessage());
                        $this->decrRequestOverplus(); // 减少剩余请求页面数

                        $this->redis->hdel($this->redisKey('retry_count'), $request); // 删除该请求失败重试次数
                    } else {
                        $this->redis->lpush($this->redisKey('queue:error'), $request); // 把请求重新重新放入队列
                        $this->redis->hincrby($this->redisKey('retry_count'), $request, 1); // 记录重试次数

                        // 爬取sleep间隔
                        $this->config['interval'] && sleep($this->config['interval']);
                    }
                },
            ]);

            // 等待爬取完成
            $pool->promise()->wait();
        }

        // 清除redis信息
        Utils::redisDel($this->redisKey('*'));
    }

    /**
     * 记录采集进度日志
     *
     */
    protected function addRequestProcessLog()
    {
        $total = $this->getRequestTotal();
        $overplus = $this->getRequestOverplus();
        $successCount = $overplus == 0 ? $total : $total - $overplus;
        if ($successCount % $this->config['log_step'] == 0) {
            $process = round(($successCount / $total), 2) * 100;
            $this->log->info('爬取进度:' . $process . '%, 已爬取页面:' . $successCount . ', 剩余页面:' . $overplus);
        }
    }

    /**
     * 添加请求错误日志
     *
     * @param $request
     * @param string $errorType
     * @param string $errorMsg
     */
    protected function addRequestErrorLog($request, $errorType = '', $errorMsg = '')
    {
        $errorInfo = [
            'prefix' => $this->redisPrefix,
            'request' => $request,
            'error_type' => $errorType,
            'error_msg' => $errorMsg,
            'error_time' => time(),
        ];
        $this->log->error('请求错误', $errorInfo);
    }

    /**
     * 处理请求
     *
     * @return void
     */
    protected function pushRequestToRedis()
    {
        $this->log->info('初始化队列 start');

        // 清除旧的redis数据
        Utils::redisDel($this->redisKey('*'));

        $lastLogProcess = 0;
        foreach ($this->config['requests']() as $key => $val) {
            $request = $this->requestFormat($val);
            if (!$this->checkUri($request['uri'])) {
                $this->log->info($request['uri'] . '不合法, 已跳过');
                continue;
            }

            $request = json_encode($request);
            // 利用sets数据结构来避免添加重复请求到队列
            if ($this->redis->sadd($this->redisKey('sets'), $request)) {
                $this->lpushRequest($request);
            }

            // 记录队列进度日志
            if ($this->config['queue_len']) {
                $curProcess = round(($key + 1) / $this->config['queue_len'], 2) * 100;
                if ($curProcess - $lastLogProcess >= 5) {
                    $this->log->info('初始化队列:' . $curProcess . '%, 队列长度:' . $this->getRequestLength());
                    $lastLogProcess = $curProcess;
                }
            }
        }
        // 清除sets数据
        $this->redis->del($this->redisKey('sets'));

        // 初始化剩余爬取的页面总数(以此来判断是否爬取完成)
        $overplus = $this->getRequestLength();
        $this->setRequestOverplus($overplus);
        $this->setRequestTotal($overplus);

        $this->log->info('初始化队列 end');
    }

    /**
     * 处理断点请求
     *
     * @return void
     */
    protected function pushContinueRequestToRedis()
    {
        foreach ($this->hgetAllRequesting() as $key => $val) {
            $this->rpushRequest($val);
        }

        // 初始化剩余爬取的页面总数(以此来判断是否爬取完成)
        $overplus = $this->getRequestLength();
        $this->setRequestOverplus($overplus);
        $this->delRequesting();
    }

    /**
     * 验证uri
     *
     * @param $uri
     * @return bool
     */
    protected function checkUri($uri)
    {
        $checkUri = (strstr($uri, 'http:') || strstr($uri, 'https:')) ? $uri : 'http:' . $uri;
        if (!filter_var($checkUri, FILTER_VALIDATE_URL)) {
            return false;
        }

        return true;
    }

    /**
     * 获取真实请求数据（传入guzzle的请求数据）
     *
     * @param array $request 请求内容
     * @return array 转化后的数据
     */
    protected function getRealRequest($request)
    {
        $request = json_decode($request, true);
        // 转化multipart中的filepath数据（上传文件数据）
        if (empty($request['multipart'])) {
            return $request;
        }

        foreach ($request['multipart'] as $key => $val) {
            if (empty($val['filepath'])) {
                continue;
            }

            $request['multipart'][$key]['contents'] = fopen($val['filepath'], 'r');
            unset($request['multipart'][$key]['filepath']);
        }
        return $request;
    }

    /**
     * 获取格式化后的请求
     * @param string|array $request 请求内容
     * @return array 格式化后的请求
     */
    protected function requestFormat($request)
    {
        // 如果请求内容为字符串/数字，则把字符串/数字当作url转为get数组请求。
        if (is_string($request) || is_numeric($request)) {
            return [
                'method' => 'get',
                'uri' => $this->config['base_uri'] . $request,
            ];
        }

        if (is_array($request)) {
            $request['uri'] = $this->config['base_uri'] . $request['uri'];
            if (empty($request['method'])) {
                $request['method'] = 'get';
            }
            return $request;
        }

        return false;
    }

    /**
     * 新增请求
     *
     * @param $request
     * @return bool|void
     */
    public function addRequest($request)
    {
        $request = $this->requestFormat($request);
        if (!isset($request['uri']) || !$this->checkUri($request['uri'])) {
            $this->log->info($request['uri'] . '不合法, 已跳过');
            return;
        }

        // 添加请求到队列
        $this->lpushRequest(json_encode($request));

        $this->redis->incr($this->redisKey('overplus'));
        $this->redis->incr($this->redisKey('total'));
        return true;
    }

    /**
     * 获取redis key
     *
     * @param $key
     * @return string
     */
    protected function redisKey($key)
    {
        $rs = $this->redisPrefix . ":{$key}";
        return $rs;
    }

    /**
     * 删除requesting请求set
     *
     */
    protected function delRequesting()
    {
        return $this->redis->del($this->redisKey('requesting'));
    }

    /**
     * 从set删除requesting请求
     *
     * @param $index
     * @return int
     */
    protected function hdelRequesting($index)
    {
        return $this->redis->hdel($this->redisKey('requesting'), $index);
    }

    /**
     * 从set获取requesting请求
     *
     * @param $index
     * @return string
     */
    protected function hgetRequesting($index)
    {
        return $this->redis->hget($this->redisKey('requesting'), $index);
    }

    /**
     * 从set获取所以requesting请求
     *
     */
    protected function hgetAllRequesting()
    {
        return $this->redis->hgetall($this->redisKey('requesting'));
    }

    /**
     * 添加request到set
     *
     * @param $index
     * @param $request
     * @return int
     */
    protected function hsetRequesting($index, $request)
    {
        return $this->redis->hset($this->redisKey('requesting'), $index, $request);
    }

    /**
     * 添加request到redis queue尾部
     *
     * @param $request
     * @return int
     */
    protected function rpushRequest($request)
    {
        return $this->redis->rpush($this->redisKey('queue'), $request);
    }

    /**
     * 添加request到redis queue头部
     *
     * @param $request
     * @return int
     */
    protected function lpushRequest($request)
    {
        return $this->redis->lpush($this->redisKey('queue'), $request);
    }

    /**
     * 请求出队列
     *
     * @return string
     */
    protected function rpopRequest()
    {
        return $this->redis->rpop($this->redisKey('queue'));
    }

    /**
     * 失败请求出队列
     *
     * @return string
     */
    protected function rpopErrorRequest()
    {
        return $this->redis->rpop($this->redisKey('queue:error'));
    }

    /**
     * 获取请求队列长度
     *
     * @return int
     */
    public function getRequestLength()
    {
        return $this->redis->llen($this->redisKey('queue'));
    }

    /**
     * 设置总爬取页数 以此来判断爬取进度
     *
     * @param $total
     * @return mixed
     */
    public function setRequestTotal($total)
    {
        return $this->redis->set($this->redisKey('total'), $total);
    }

    /**
     * 获取总爬取页数
     *
     * @return string
     */
    public function getRequestTotal()
    {
        return $this->redis->get($this->redisKey('total'));
    }

    /**
     * 设置剩余爬取数
     *
     * @param $total
     * @return mixed
     */
    public function setRequestOverplus($total)
    {
        return $this->redis->set($this->redisKey('overplus'), $total);
    }

    /**
     * 剩余数自减
     *
     */
    public function decrRequestOverplus()
    {
        return $this->redis->decr($this->redisKey('overplus'));
    }

    /**
     * 获取剩余爬取页数
     *
     * @return string
     */
    public function getRequestOverplus()
    {
        return $this->redis->get($this->redisKey('overplus'));
    }
}