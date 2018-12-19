<?php

namespace Zblues\Codeigniter;

class Ezdb
{

    /**
     * Create a new Skeleton Instance
     */
    public function __construct($this->ci)
    {
        $this->ci = $this->ci;
    }

    /**
     * Friendly welcome
     *
     * @param string $phrase Phrase to return
     *
     * @return string Returns the phrase passed in
     */
    public function open($dbconfig)
    {
        $config = array(
            'dsn'	=> '',
            'hostname' => '',
            'username' => '',
            'password' => '',
            'database' => '',
            'dbdriver' => 'pdo',
            'dbprefix' => '',
            'pconnect' => FALSE,
            'db_debug' => (ENVIRONMENT !== 'production'),
            'cache_on' => FALSE,
            'cachedir' => '',
            'char_set' => 'utf8',
            'dbcollat' => 'utf8_unicode_ci',
            'swap_pre' => '',
            'encrypt' => FALSE,
            'compress' => FALSE,
            'stricton' => FALSE,
            'failover' => array(),
            'save_queries' => TRUE
        );
        
        foreach($dbconfig as $key=>$val) $config[$key] = $val;
        
        if(empty($dbconfig['dsn']))
            $config['dsn'] = "{$dbconfig['dbtype']}:host={$dbconfig['hostname']};port=3306;dbname={$dbconfig['database']};charset=utf8";
        
        return $this->ci->load->database($config, true);
    }

    function close($db)
    {
        $db->close();
    }
    
    function beginTransaction($mode='auto', $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        
        if($mode=='auto') $db->trans_start();
        else $db->trans_begin();
    }

    function endTransaction($mode='auto', $status=true, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        
        if($mode=='auto') {
            $db->trans_complete();
            return $db->trans_status();
        } 
        // mode == manual
        else {
            $ret = $this->ci->db->trans_status();
            if($ret==true) {
                if($status==true) {
                    $this->ci->db->trans_commit();
                    return true;
                }
                else {
                    $this->ci->db->trans_rollback();
                    return false;
                }
            } else {
                $this->ci->db->trans_rollback();
                return false;
            }
        }
    }

    function retmsg($ret, $successMsg="정상적으로 처리되었습니다.", $failMsg="처리중 에러가 발생했습니다.")
    {
        if($ret===false) return [
            'success'   => 0, 
            'msg'       => $failMsg
        ];
        else return [
            'success'   => 1, 
            'msg'       => $successMsg
        ];
    }
    
    function errorMessage($db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
    #log_message('debug', __METHOD__ . ': ' . var_export($db,true));
        //return $db->_error_message();
        $err = $db->error();
        return $err['message'] . '(' . $err['code'] . ')';
    }

    function query($qry, $db='',$useBufferedQuery=true)
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        $db->conn_id->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, $useBufferedQuery);
        return $db->query($qry);
    }

    // $sth : query(), prepare()로 리턴된 값
    function fetch(&$sth)
    {
        return $sth->unbuffered_row('array');
    }

    function exec($qry, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        return $db->exec($qry);
    }

    // $mode : array, object
    function select($qry, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        
        $res = $db->query($qry);
        if($res===false) return false;
        
        return $res->result_array();
    }

    function selectOne($qry, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        
        $res = $db->query($qry);
        if($res===false) return false;
        
        return $res->row_array();
    }

    // 사용예: $cnt = select_data("SELECT COUNT(*) FROM some_table");
    function selectData($qry, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }

        $res = $db->query($qry);
        if($res===false) return false;
        
        $row = $res->row_array();
    #log_message('debug', __METHOD__ . ': ' . var_export($row,true));	
        // reset($row)는 array_shift()를 이용한 것과 동일한 역할을 함
        return empty($row) ? '' : reset($row);
    }

    function insert($table, $data, $db='')
    {
        if(empty($db)) {
            $this->ci =& get_instance();
            $db = $this->ci->db;
        }
        
        $fieldDetails = NULL;
        foreach($data as $key=> $value) {
            if(substr($value,0,2)=='-|' && substr($value,-2)=='|-') {
                $fieldDetails .= "`$key`=" . substr($value,2,-2) . ',';
                unset($data[$key]);
            }
            else $fieldDetails .= "`$key`=?,";
        }
        $fieldDetails = rtrim($fieldDetails, ',');
        
        $qry = "INSERT INTO {$table} SET {$fieldDetails}";
        $ret = $db->query($qry, $data);
        if($ret===false) return false;
        
        return $db->insert_id();
    }

    function update($table, $data, $where, $db='')
    {
        if(empty($db)) {
            $db = $this->ci->db;
        }
        
        $fieldDetails = NULL;
        foreach($data as $key=> $value) {
            if(substr($value,0,2)=='-|' && substr($value,-2)=='|-') {
                $fieldDetails .= "`$key`=" . substr($value,2,-2) . ',';
                unset($data[$key]);
            }
            else $fieldDetails .= "`$key`=?,";
        }
        $fieldDetails = rtrim($fieldDetails, ',');
        
        $qry = "UPDATE {$table} SET {$fieldDetails} WHERE {$where}";
    #log_message('debug', __METHOD__ . ': ' . $qry);
        $ret = $db->query($qry, $data);
        if($ret===false) return false;

        return $db->affected_rows();
    }

    function delete($table, $where, $db='')
    {
        if(empty($db)) {
            $this->ci =& get_instance();
            $db = $this->ci->db;
        }

        $qry = "DELETE FROM {$table} WHERE {$where}";
        return $db->query($qry);
    }

    function bulkInsert($table, $data, $maxRows=1000, $db='')
    {
        if(empty($db)) {
            $this->ci =& get_instance();
            $db = $this->ci->db;
        }

        $totalRows  = count($data);
        $start = 0;
        $length = $maxRows;

        while(1) {
            $arr = array_slice($data, $start, $length);
            $ret = $db->insert_batch($table, $arr);

            $start += $length;

            if($totalRows < $start) break;
        }

        return;
    }
}
