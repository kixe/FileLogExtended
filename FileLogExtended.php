<?php namespace ProcessWire;

/**
 * Helper class for FileLog which sourced out a function including PHP::flock() used by FileLog::save(), FileLog::find() and FileLog::pruneBytes(). Recommended if heavily accessed due to frequent save() requests
 * 
 */
class FileLogExtended extends FileLog {

    /**
     * Save the given log entry string
     * 
     * @param string $str
     * @param array $options options to modify behavior (Added 3.0.143)
     *  - `allowDups` (bool): Allow duplicating same log entry in same runtime/request? (default=true) 
     *  - `mergeDups` (int): Merge previous duplicate entries that also appear near end of file?
     *     To enable, specify int for quantity of bytes to consider from EOF, value of 1024 or higher (default=0, disabled) 
     *  - `maxTries` (int): If log entry fails to save, maximum times to re-try (default=20) 
     *  - `maxTriesDelay` (int): Micro seconds (millionths of a second) to delay between re-tries (default=2000)
     * @return bool Success state: true if log written, false if not. 
     * 
     */
    public function save($str, array $options = array()) {
        
        $defaults = array(
            'mergeDups' => 0,
            'allowDups' => true, 
            'maxTries' => 20, 
            'maxTriesDelay' => 2000, 
        );

        if(!$this->logFilename) return false;
        
        $options = array_merge($defaults, $options);
        $hash = md5($str); 
        $ts = date("Y-m-d H:i:s"); 
        $str = $this->cleanStr($str);
        $line = $this->delimeter . $str; // log entry, excluding timestamp
        $hasLock = false; // becomes true when lock obtained
        $fp = false; // becomes resource when file is open
        
        // if we've already logged this during this instance, then don't do it again
        if(!$options['allowDups'] && isset($this->itemsLogged[$hash])) return true;

        // determine write mode     
        $mode = file_exists($this->logFilename) ? 'a' : 'w';
        if($mode === 'a' && $options['mergeDups']) $mode = 'r+';

        // open the log file
        for($tries = 0; $tries <= $options['maxTries']; $tries++) {
            $fp = fopen($this->logFilename, $mode);
            if($fp) break;
            // if unable to open for reading/writing, see if we can open for append instead
            if($mode === 'r+' && $tries > ($options['maxTries'] / 2)) $mode = 'a';
            usleep($options['maxTriesDelay']);
        }

        // if unable to open, exit now
        if(!$fp) return false;

        // obtain a lock, if unable to obtain a lock, we cannot write to the log
        if (!$this->lockFile($fp, $options['maxTries'], $options['maxTriesDelay'])) return false;

        // if opened for reading and writing, merge duplicates of $line
        if($mode === 'r+' && $options['mergeDups']) {
            // do not repeat the same log entry in the same chunk
            $chunkSize = (int) $options['mergeDups']; 
            if($chunkSize < 1024) $chunkSize = 1024;
            fseek($fp, -1 * $chunkSize, SEEK_END);
            $chunk = fread($fp, $chunkSize); 
            // check if our log line already appears in the immediate earlier chunk
            if(strpos($chunk, $line) !== false) {
                // this log entry already appears 1+ times within the last chunk of the file
                // remove the duplicates and replace the chunk
                $chunkLength = strlen($chunk);
                $this->removeLineFromChunk($line, $chunk, $chunkSize);
                fseek($fp, 0, SEEK_END);
                $oldLength = ftell($fp);
                $newLength = $chunkLength > $oldLength ? $oldLength - $chunkLength : 0;
                ftruncate($fp, $newLength); 
                fseek($fp, 0, SEEK_END);
                fwrite($fp, $chunk);
            }   
        } else {
            // already at EOF because we are appending or creating
        }
        
        // add the log line
        $result = fwrite($fp, "$ts$line\n");
        
        // release the lock and close the file
        flock($fp, LOCK_UN);
        fclose($fp);
        
        if($result && !$options['allowDups']) $this->itemsLogged[$hash] = true;
        
        // if we were creating the file, make sure it has the right permission
        if($mode === 'w') {
            $files = $this->wire()->files;
            $files->chmod($this->logFilename);
        }
        
        return (int) $result > 0; 
    }

    /**
     * Return lines from the end of the log file, with various options
     *
     * @param int $limit Number of items to return (per pagination), or 0 for no limit.
     * @param int $pageNum Current pagination (default=1)
     * @param array $options
     *  - text (string): Return only lines containing the given string of text
     *  - reverse (bool): True=find from end of file, false=find from beginning (default=true)
     *  - toFile (string): Send results to the given basename (default=none)
     *  - dateFrom (unix timestamp): Return only lines newer than the given date (default=oldest)
     *  - dateTo (unix timestamp): Return only lines older than the given date  (default=now)
     *      Note: dateFrom and dateTo may be combined to return a range.
     * @return int|array of strings (associative), each indexed by string containing slash separated 
     *  numeric values of: "current/total/start/end/total" which is useful with pagination.
     *  If the 'toFile' option is used, then return value is instead an integer qty of lines written.
     * @throws \Exception on fatal error
     * 
     */
    public function find($limit = 100, $pageNum = 1, array $options = array()) {
        
        $defaults = array(
            'text' => null, 
            'dateFrom' => 0,
            'dateTo' => 0,
            'reverse' => true, 
            'toFile' => '', 
        );
        
        $options = array_merge($defaults, $options); 
        $hasFilters = !empty($options['text']);

        if($options['dateFrom'] || $options['dateTo']) {
            if(!$options['dateTo']) $options['dateTo'] = time();
            if(!ctype_digit("$options[dateFrom]")) $options['dateFrom'] = strtotime($options['dateFrom']);
            if(!ctype_digit("$options[dateTo]")) $options['dateTo'] = strtotime($options['dateTo']);
            $hasFilters = true; 
        }
        
        if($options['toFile']) {
            $toFile = $this->path() . basename($options['toFile']); 
            $fp = fopen($toFile, 'w'); 
            if(!$fp || !$this->lockFile($fpw)) throw new \Exception("Unable to open and lock file for writing: $toFile");
        } else {
            $toFile = '';
            $fp = null;
        }
        
        $lines = array();
        $start = ($pageNum-1) * $limit; 
        $end = $start + $limit; 
        $cnt = 0; // number that will be written or returned by this
        $n = 0; // number total
        $chunkNum = 0;
        $totalChunks = $this->getTotalChunks($this->chunkSize); 
        $stopNow = false;
        $chunkLineHashes = array();
        
        while($chunkNum <= $totalChunks && !$stopNow) {
            
            $chunk = $this->getChunkArray(++$chunkNum, 0, $options['reverse']);
            if(empty($chunk)) break;
            
            foreach($chunk as $line) {

                $line = trim($line); 
                $hash = md5($line); 
                $valid = !isset($chunkLineHashes[$hash]);
                $chunkLineHashes[$hash] = 1; 
                if($valid) $valid = $this->isValidLine($line, $options, $stopNow);
                if(!$hasFilters && $limit && count($lines) >= $limit) $stopNow = true;
                if($stopNow) break;
                if(!$valid) continue; 
                
                $n++;
                if($limit && ($n <= $start || $n > $end)) continue; 
                $cnt++;
                if($fp) {
                    fwrite($fp, $line . "\n");
                } else {
                    if(self::debug) $line .= " (line $n, chunk $chunkNum, hash=$hash)";
                    $lines[$n] = $line;
                }
            }
        }
        
        $total = $hasFilters ? $n : $this->getTotalLines();
        $end = $start + count($lines); 
        if($end > $total) $end = $total;
        if(count($lines) < $limit && $total > $end) $total = $end; 
        
        if($fp) {
            fclose($fp);
            $this->wire()->files->chmod($toFile); 
            return $cnt;
        }
            
        foreach($lines as $key => $line) {
            unset($lines[$key]);
            $lines["$key/$total/$start/$end/$limit"] = $line;
        }
        return $lines; 
    }

    /**
     * Prune log file to specified number of bytes (from the end)
     * same like in parent class but with additional function fileLockCheck()
     * to prevent files-error triggered by WireFileTools::unlink() and WireFileTools::rename()
     * 
     * @param int $bytes
     * @return int|bool positive integer on success, 0 if no prune necessary, false if another process holds the log, or on failure.
     * 
     */
    public function pruneBytes($bytes) {
        $filename = $this->logFilename; 

        if(!$filename || !file_exists($filename) || filesize($filename) <= $bytes) return 0; 

        $fpr = fopen($filename, "r");
        $fpw = fopen("$filename.new", "w");  
        if (!$this->lockFile($fpw)) return false;

        if(!$fpr || !$fpw) return false;

        fseek($fpr, ($bytes * -1), SEEK_END); 
        fgets($fpr, $this->maxLineLength); // first line likely just a partial line, so skip it
        $cnt = 0;

        while(!feof($fpr)) {
            $line = fgets($fpr, $this->maxLineLength); 
            fwrite($fpw, $line); 
            $cnt++;
        }

        // unlock & close files
        fclose($fpw);
        fclose($fpr); 
        
        $files = $this->wire()->files;

        if($cnt) {
            $files->unlink($filename, true);
            $files->rename("$filename.new", $filename, true);
            $files->chmod($filename); 
        } else {
            $files->unlink("$filename.new", true);
        }
    
        return $cnt;    
    }

    /**
     * lock a file, should be unlocked later on success
     * 
     * @param Resource file pointer $fpr
     * @param int $maxTries
     * @param int $maxTriesDelay
     * @return bool true if file could be locked by this thread, false if locked by another
     * @throws WireException if log file is locked for unknown reasons
     * 
     */
    protected function lockFile($fp, $maxTries = 5, $maxTriesDelay = 1000) {
        return true;
        for($tries = 0; $tries <= $maxTries; $tries++) {
            if (!flock($fp, LOCK_EX|LOCK_NB, $wouldblock)) {    
                if ($wouldblock) {
                    // unable to lock, another process holds the lock ($wouldblock == 1)
                    usleep($maxTriesDelay);
                } else {
                    // couldn't lock for another reason, e.g. no such file
                    fclose($fp);
                    throw new WireException('Broken file handle. Couldn\'t lock file for unknown reason.');
                }
            } else {
                // lock obtained, should be unlocked later calling fclose($fpr) or flock($fpr, LOCK_UN)
                return true;
            }
        }
        fclose($fp);
        return false;
    }
}