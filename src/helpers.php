<?php
/**
 * 把jsonp转为php数组
 *
 * @param string $jsonp jsonp字符串
 * @param boolean $assoc 当该参数为true时，将返回array而非object
 * @return array
 */
function jsonp_decode($jsonp, $assoc = true)
{
    $tmpArr = [];
    if(!preg_match('#^.*\((.*)\).*$#isU', trim($jsonp), $tmpArr)){
        return false;
    }

    $json = $tmpArr[1];
    return json_decode($json, $assoc);
}