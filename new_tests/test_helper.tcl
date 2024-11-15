# Server test suite. Copyright (C) 2009 Redis Ltd.
# This software is released under the BSD License. See the COPYING file for
# more information.

package require Tcl 8.6

set tcl_precision 17

# Tcl client library - used by the server test
# Copyright (c) 2009-2014 Redis Ltd.
# Released under the BSD license like Redis itself
#
# Example usage:
#
# set r [valkey 127.0.0.1 6379]
# $r lpush mylist foo
# $r lpush mylist bar
# $r lrange mylist 0 -1
# $r close
#
# Non blocking usage example:
#
# proc handlePong {r type reply} {
#     puts "PONG $type '$reply'"
#     if {$reply ne "PONG"} {
#         $r ping [list handlePong]
#     }
# }
#
# set r [valkey]
# $r blocking 0
# $r get fo [list handlePong]
#
# vwait forever

# Tcl client library - used by the server test
# Copyright (C) 2009-2023 Redis Ltd.
# Released under the BSD license like Redis itself
#
# This file contains a bunch of commands whose purpose is to transform
# a RESP3 response to RESP2
# Why is it needed?
# When writing the reply_schema part in COMMAND DOCS we decided to use
# the existing tests in order to verify the schemas (see logreqres.c)
# The problem was that many tests were relying on the RESP2 structure
# of the response (e.g. HRANDFIELD WITHVALUES in RESP2: {f1 v1 f2 v2}
# vs. RESP3: {{f1 v1} {f2 v2}}).
# Instead of adjusting the tests to expect RESP3 responses (a lot of
# changes in many files) we decided to transform the response to RESP2
# when running with --force-resp3

namespace eval response_transformers {}

# Transform a map response into an array of tuples (tuple = array with 2 elements)
# Used for XREAD[GROUP]
proc transform_map_to_tuple_array {argv response} {
    set tuparray {}
    foreach {key val} $response {
        set tmp {}
        lappend tmp $key
        lappend tmp $val
        lappend tuparray $tmp
    }
    return $tuparray
}

# Transform an array of tuples to a flat array
proc transform_tuple_array_to_flat_array {argv response} {
    set flatarray {}
    foreach pair $response {
        lappend flatarray {*}$pair
    }
    return $flatarray
}

# With HRANDFIELD, we only need to transform the response if the request had WITHVALUES
# (otherwise the returned response is a flat array in both RESPs)
proc transform_hrandfield_command {argv response} {
    foreach ele $argv {
        if {[string compare -nocase $ele "WITHVALUES"] == 0} {
            return [transform_tuple_array_to_flat_array $argv $response]
        }
    }
    return $response
}

# With some zset commands, we only need to transform the response if the request had WITHSCORES
# (otherwise the returned response is a flat array in both RESPs)
proc transform_zset_withscores_command {argv response} {
    foreach ele $argv {
        if {[string compare -nocase $ele "WITHSCORES"] == 0} {
            return [transform_tuple_array_to_flat_array $argv $response]
        }
    }
    return $response
}

# With ZPOPMIN/ZPOPMAX, we only need to transform the response if the request had COUNT (3rd arg)
# (otherwise the returned response is a flat array in both RESPs)
proc transform_zpopmin_zpopmax {argv response} {
    if {[llength $argv] == 3} {
        return [transform_tuple_array_to_flat_array $argv $response]
    }
    return $response
}

set ::transformer_funcs {
    XREAD transform_map_to_tuple_array
    XREADGROUP transform_map_to_tuple_array
    HRANDFIELD transform_hrandfield_command
    ZRANDMEMBER transform_zset_withscores_command
    ZRANGE transform_zset_withscores_command
    ZRANGEBYSCORE transform_zset_withscores_command
    ZRANGEBYLEX transform_zset_withscores_command
    ZREVRANGE transform_zset_withscores_command
    ZREVRANGEBYSCORE transform_zset_withscores_command
    ZREVRANGEBYLEX transform_zset_withscores_command
    ZUNION transform_zset_withscores_command
    ZDIFF transform_zset_withscores_command
    ZINTER transform_zset_withscores_command
    ZPOPMIN transform_zpopmin_zpopmax
    ZPOPMAX transform_zpopmin_zpopmax
}

proc ::response_transformers::transform_response_if_needed {id argv response} {
    if {![::valkey::should_transform_to_resp2 $id] || $::valkey::readraw($id)} {
        return $response
    }

    set key [string toupper [lindex $argv 0]]
    if {![dict exists $::transformer_funcs $key]} {
        return $response
    }

    set transform [dict get $::transformer_funcs $key]

    return [$transform $argv $response]
}

namespace eval valkey {}
set ::valkey::id 0
array set ::valkey::fd {}
array set ::valkey::addr {}
array set ::valkey::blocking {}
array set ::valkey::deferred {}
array set ::valkey::readraw {}
array set ::valkey::attributes {} ;# Holds the RESP3 attributes from the last call
array set ::valkey::reconnect {}
array set ::valkey::tls {}
array set ::valkey::callback {}
array set ::valkey::state {} ;# State in non-blocking reply reading
array set ::valkey::statestack {} ;# Stack of states, for nested mbulks
array set ::valkey::curr_argv {} ;# Remember the current argv, to be used in response_transformers.tcl
array set ::valkey::testing_resp3 {} ;# Indicating if the current client is using RESP3 (only if the test is trying to test RESP3 specific behavior. It won't be on in case of force_resp3)

set ::force_resp3 0
set ::log_req_res 0

proc valkey {{server 127.0.0.1} {port 6379} {defer 0} {tls 0} {tlsoptions {}} {readraw 0}} {
    if {$tls} {
        package require tls
        ::tls::init \
            -cafile "$::tlsdir/ca.crt" \
            -certfile "$::tlsdir/client.crt" \
            -keyfile "$::tlsdir/client.key" \
            {*}$tlsoptions
        set fd [::tls::socket $server $port]
    } else {
        set fd [socket $server $port]
    }
    fconfigure $fd -translation binary
    set id [incr ::valkey::id]
    set ::valkey::fd($id) $fd
    set ::valkey::addr($id) [list $server $port]
    set ::valkey::blocking($id) 1
    set ::valkey::deferred($id) $defer
    set ::valkey::readraw($id) $readraw
    set ::valkey::reconnect($id) 0
    set ::valkey::curr_argv($id) 0
    set ::valkey::testing_resp3($id) 0
    set ::valkey::tls($id) $tls
    ::valkey::valkey_reset_state $id
    interp alias {} ::valkey::valkeyHandle$id {} ::valkey::__dispatch__ $id
}

# On recent versions of tcl-tls/OpenSSL, reading from a dropped connection
# results with an error we need to catch and mimic the old behavior.
proc ::valkey::valkey_safe_read {fd len} {
    if {$len == -1} {
        set err [catch {set val [read $fd]} msg]
    } else {
        set err [catch {set val [read $fd $len]} msg]
    }
    if {!$err} {
        return $val
    }
    if {[string match "*connection abort*" $msg]} {
        return {}
    }
    error $msg
}

proc ::valkey::valkey_safe_gets {fd} {
    if {[catch {set val [gets $fd]} msg]} {
        if {[string match "*connection abort*" $msg]} {
            return {}
        }
        error $msg
    }
    return $val
}

# This is a wrapper to the actual dispatching procedure that handles
# reconnection if needed.
proc ::valkey::__dispatch__ {id method args} {
    set errorcode [catch {::valkey::__dispatch__raw__ $id $method $args} retval]
    if {$errorcode && $::valkey::reconnect($id) && $::valkey::fd($id) eq {}} {
        # Try again if the connection was lost.
        # FIXME: we don't re-select the previously selected DB, nor we check
        # if we are inside a transaction that needs to be re-issued from
        # scratch.
        set errorcode [catch {::valkey::__dispatch__raw__ $id $method $args} retval]
    }
    return -code $errorcode $retval
}

proc ::valkey::__dispatch__raw__ {id method argv} {
    set fd $::valkey::fd($id)

    # Reconnect the link if needed.
    if {$fd eq {} && $method ne {close}} {
        lassign $::valkey::addr($id) host port
        if {$::valkey::tls($id)} {
            set ::valkey::fd($id) [::tls::socket $host $port]
        } else {
            set ::valkey::fd($id) [socket $host $port]
        }
        fconfigure $::valkey::fd($id) -translation binary
        set fd $::valkey::fd($id)
    }

    # Transform HELLO 2 to HELLO 3 if force_resp3
    # All set the connection var testing_resp3 in case of HELLO 3
    if {[llength $argv] > 0 && [string compare -nocase $method "HELLO"] == 0} {
        if {[lindex $argv 0] == 3} {
            set ::valkey::testing_resp3($id) 1
        } else {
            set ::valkey::testing_resp3($id) 0
            if {$::force_resp3} {
                # If we are in force_resp3 we run HELLO 3 instead of HELLO 2
                lset argv 0 3
            }
        }
    }

    set blocking $::valkey::blocking($id)
    set deferred $::valkey::deferred($id)
    if {$blocking == 0} {
        if {[llength $argv] == 0} {
            error "Please provide a callback in non-blocking mode"
        }
        set callback [lindex $argv end]
        set argv [lrange $argv 0 end-1]
    }
    if {[info command ::valkey::__method__$method] eq {}} {
        catch {unset ::valkey::attributes($id)}
        set cmd "*[expr {[llength $argv]+1}]\r\n"
        append cmd "$[string length $method]\r\n$method\r\n"
        foreach a $argv {
            append cmd "$[string length $a]\r\n$a\r\n"
        }
        ::valkey::valkey_write $fd $cmd
        if {[catch {flush $fd}]} {
            catch {close $fd}
            set ::valkey::fd($id) {}
            return -code error "I/O error reading reply"
        }

        set ::valkey::curr_argv($id) [concat $method $argv]
        if {!$deferred} {
            if {$blocking} {
                ::valkey::valkey_read_reply $id $fd
            } else {
                # Every well formed reply read will pop an element from this
                # list and use it as a callback. So pipelining is supported
                # in non blocking mode.
                lappend ::valkey::callback($id) $callback
                fileevent $fd readable [list ::valkey::valkey_readable $fd $id]
            }
        }
    } else {
        uplevel 1 [list ::valkey::__method__$method $id $fd] $argv
    }
}

proc ::valkey::__method__blocking {id fd val} {
    set ::valkey::blocking($id) $val
    fconfigure $fd -blocking $val
}

proc ::valkey::__method__reconnect {id fd val} {
    set ::valkey::reconnect($id) $val
}

proc ::valkey::__method__read {id fd} {
    ::valkey::valkey_read_reply $id $fd
}

proc ::valkey::__method__rawread {id fd {len -1}} {
    return [valkey_safe_read $fd $len]
}

proc ::valkey::__method__write {id fd buf} {
    ::valkey::valkey_write $fd $buf
}

proc ::valkey::__method__flush {id fd} {
    flush $fd
}

proc ::valkey::__method__close {id fd} {
    catch {close $fd}
    catch {unset ::valkey::fd($id)}
    catch {unset ::valkey::addr($id)}
    catch {unset ::valkey::blocking($id)}
    catch {unset ::valkey::deferred($id)}
    catch {unset ::valkey::readraw($id)}
    catch {unset ::valkey::attributes($id)}
    catch {unset ::valkey::reconnect($id)}
    catch {unset ::valkey::tls($id)}
    catch {unset ::valkey::state($id)}
    catch {unset ::valkey::statestack($id)}
    catch {unset ::valkey::callback($id)}
    catch {unset ::valkey::curr_argv($id)}
    catch {unset ::valkey::testing_resp3($id)}
    catch {interp alias {} ::valkey::valkeyHandle$id {}}
}

proc ::valkey::__method__channel {id fd} {
    return $fd
}

proc ::valkey::__method__deferred {id fd val} {
    set ::valkey::deferred($id) $val
}

proc ::valkey::__method__readraw {id fd val} {
    set ::valkey::readraw($id) $val
}

proc ::valkey::__method__readingraw {id fd} {
    return $::valkey::readraw($id)
}

proc ::valkey::__method__attributes {id fd} {
    set _ $::valkey::attributes($id)
}

proc ::valkey::valkey_write {fd buf} {
    puts -nonewline $fd $buf
}

proc ::valkey::valkey_writenl {fd buf} {
    valkey_write $fd $buf
    valkey_write $fd "\r\n"
    flush $fd
}

proc ::valkey::valkey_readnl {fd len} {
    set buf [valkey_safe_read $fd $len]
    valkey_safe_read $fd 2 ; # discard CR LF
    return $buf
}

proc ::valkey::valkey_bulk_read {fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set buf [valkey_readnl $fd $count]
    return $buf
}

proc ::valkey::redis_multi_bulk_read {id fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set l {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            lappend l [valkey_read_reply_logic $id $fd]
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $l
}

proc ::valkey::valkey_read_map {id fd} {
    set count [valkey_read_line $fd]
    if {$count == -1} return {}
    set d {}
    set err {}
    for {set i 0} {$i < $count} {incr i} {
        if {[catch {
            set k [valkey_read_reply_logic $id $fd] ; # key
            set v [valkey_read_reply_logic $id $fd] ; # value
            dict set d $k $v
        } e] && $err eq {}} {
            set err $e
        }
    }
    if {$err ne {}} {return -code error $err}
    return $d
}

proc ::valkey::valkey_read_line fd {
    string trim [valkey_safe_gets $fd]
}

proc ::valkey::valkey_read_null fd {
    valkey_safe_gets $fd
    return {}
}

proc ::valkey::valkey_read_bool fd {
    set v [valkey_read_line $fd]
    if {$v == "t"} {return 1}
    if {$v == "f"} {return 0}
    return -code error "Bad protocol, '$v' as bool type"
}

proc ::valkey::valkey_read_double {id fd} {
    set v [valkey_read_line $fd]
    # unlike many other DTs, there is a textual difference between double and a string with the same value,
    # so we need to transform to double if we are testing RESP3 (i.e. some tests check that a
    # double reply is "1.0" and not "1")
    if {[should_transform_to_resp2 $id]} {
        return $v
    } else {
        return [expr {double($v)}]
    }
}

proc ::valkey::valkey_read_verbatim_str fd {
    set v [valkey_bulk_read $fd]
    # strip the first 4 chars ("txt:")
    return [string range $v 4 end]
}

proc ::valkey::valkey_read_reply_logic {id fd} {
    if {$::valkey::readraw($id)} {
        return [valkey_read_line $fd]
    }

    while {1} {
        set type [valkey_safe_read $fd 1]
        switch -exact -- $type {
            _ {return [valkey_read_null $fd]}
            : -
            ( -
            + {return [valkey_read_line $fd]}
            , {return [valkey_read_double $id $fd]}
            # {return [valkey_read_bool $fd]}
            = {return [valkey_read_verbatim_str $fd]}
            - {return -code error [valkey_read_line $fd]}
            $ {return [valkey_bulk_read $fd]}
            > -
            ~ -
            * {return [redis_multi_bulk_read $id $fd]}
            % {return [valkey_read_map $id $fd]}
            | {
                set attrib [valkey_read_map $id $fd]
                set ::valkey::attributes($id) $attrib
                continue
            }
            default {
                if {$type eq {}} {
                    catch {close $fd}
                    set ::valkey::fd($id) {}
                    return -code error "I/O error reading reply"
                }
                return -code error "Bad protocol, '$type' as reply type byte"
            }
        }
    }
}

proc ::valkey::valkey_read_reply {id fd} {
    set response [valkey_read_reply_logic $id $fd]
    ::response_transformers::transform_response_if_needed $id $::valkey::curr_argv($id) $response
}

proc ::valkey::valkey_reset_state id {
    set ::valkey::state($id) [dict create buf {} mbulk -1 bulk -1 reply {}]
    set ::valkey::statestack($id) {}
}

proc ::valkey::valkey_call_callback {id type reply} {
    set cb [lindex $::valkey::callback($id) 0]
    set ::valkey::callback($id) [lrange $::valkey::callback($id) 1 end]
    uplevel #0 $cb [list ::valkey::valkeyHandle$id $type $reply]
    ::valkey::valkey_reset_state $id
}

# Read a reply in non-blocking mode.
proc ::valkey::valkey_readable {fd id} {
    if {[eof $fd]} {
        valkey_call_callback $id eof {}
        ::valkey::__method__close $id $fd
        return
    }
    if {[dict get $::valkey::state($id) bulk] == -1} {
        set line [gets $fd]
        if {$line eq {}} return ;# No complete line available, return
        switch -exact -- [string index $line 0] {
            : -
            + {valkey_call_callback $id reply [string range $line 1 end-1]}
            - {valkey_call_callback $id err [string range $line 1 end-1]}
            ( {valkey_call_callback $id reply [string range $line 1 end-1]}
            $ {
                dict set ::valkey::state($id) bulk \
                    [expr [string range $line 1 end-1]+2]
                if {[dict get $::valkey::state($id) bulk] == 1} {
                    # We got a $-1, hack the state to play well with this.
                    dict set ::valkey::state($id) bulk 2
                    dict set ::valkey::state($id) buf "\r\n"
                    ::valkey::valkey_readable $fd $id
                }
            }
            * {
                dict set ::valkey::state($id) mbulk [string range $line 1 end-1]
                # Handle *-1
                if {[dict get $::valkey::state($id) mbulk] == -1} {
                    valkey_call_callback $id reply {}
                }
            }
            default {
                valkey_call_callback $id err \
                    "Bad protocol, $type as reply type byte"
            }
        }
    } else {
        set totlen [dict get $::valkey::state($id) bulk]
        set buflen [string length [dict get $::valkey::state($id) buf]]
        set toread [expr {$totlen-$buflen}]
        set data [read $fd $toread]
        set nread [string length $data]
        dict append ::valkey::state($id) buf $data
        # Check if we read a complete bulk reply
        if {[string length [dict get $::valkey::state($id) buf]] ==
            [dict get $::valkey::state($id) bulk]} {
            if {[dict get $::valkey::state($id) mbulk] == -1} {
                valkey_call_callback $id reply \
                    [string range [dict get $::valkey::state($id) buf] 0 end-2]
            } else {
                dict with ::valkey::state($id) {
                    lappend reply [string range $buf 0 end-2]
                    incr mbulk -1
                    set bulk -1
                }
                if {[dict get $::valkey::state($id) mbulk] == 0} {
                    valkey_call_callback $id reply \
                        [dict get $::valkey::state($id) reply]
                }
            }
        }
    }
}

# when forcing resp3 some tests that rely on resp2 can fail, so we have to translate the resp3 response to resp2
proc ::valkey::should_transform_to_resp2 {id} {
    return [expr {$::force_resp3 && !$::valkey::testing_resp3($id)}]
}


set ::base_aof_sufix ".base"
set ::incr_aof_sufix ".incr"
set ::manifest_suffix ".manifest"
set ::aof_format_suffix ".aof"
set ::rdb_format_suffix ".rdb"

proc get_full_path {dir filename} {
    set _ [format "%s/%s" $dir $filename]
}

proc join_path {dir1 dir2} {
    return [format "%s/%s" $dir1 $dir2]
}

proc get_valkey_dir {} {
    set config [srv config]
    set _ [dict get $config "dir"]
}

proc check_file_exist {dir filename} {
    set file_path [get_full_path $dir $filename]
    return [file exists $file_path]
}

proc del_file {dir filename} {
    set file_path [get_full_path $dir $filename]
    catch {exec rm -rf $file_path}
}

proc get_cur_base_aof_name {manifest_filepath} {
    set fp [open $manifest_filepath r+]
    set lines {}
    while {1} {
        set line [gets $fp]
        if {[eof $fp]} {
           close $fp
           break;
        }

        lappend lines $line
    }

    if {[llength $lines] == 0} {
        return ""
    }

    set first_line [lindex $lines 0]
    set aofname [lindex [split $first_line " "] 1]
    set aoftype [lindex [split $first_line " "] 5]
    if { $aoftype eq "b" } {
        return $aofname
    }

    return ""
}

proc get_last_incr_aof_name {manifest_filepath} {
    set fp [open $manifest_filepath r+]
    set lines {}
    while {1} {
        set line [gets $fp]
        if {[eof $fp]} {
           close $fp
           break;
        }

        lappend lines $line
    }

    if {[llength $lines] == 0} {
        return ""
    }

    set len [llength $lines]
    set last_line [lindex $lines [expr $len - 1]]
    set aofname [lindex [split $last_line " "] 1]
    set aoftype [lindex [split $last_line " "] 5]
    if { $aoftype eq "i" } {
        return $aofname
    }

    return ""
}

proc get_last_incr_aof_path {r} {
    set dir [lindex [$r config get dir] 1]
    set appenddirname [lindex [$r config get appenddirname] 1]
    set appendfilename [lindex [$r config get appendfilename] 1]
    set manifest_filepath [file join $dir $appenddirname $appendfilename$::manifest_suffix]
    set last_incr_aof_name [get_last_incr_aof_name $manifest_filepath]
    if {$last_incr_aof_name == ""} {
        return ""
    }
    return [file join $dir $appenddirname $last_incr_aof_name]
}

proc get_base_aof_path {r} {
    set dir [lindex [$r config get dir] 1]
    set appenddirname [lindex [$r config get appenddirname] 1]
    set appendfilename [lindex [$r config get appendfilename] 1]
    set manifest_filepath [file join $dir $appenddirname $appendfilename$::manifest_suffix]
    set cur_base_aof_name [get_cur_base_aof_name $manifest_filepath]
    if {$cur_base_aof_name == ""} {
        return ""
    }
    return [file join $dir $appenddirname $cur_base_aof_name]
}

proc assert_aof_manifest_content {manifest_path content} {
    set fp [open $manifest_path r+]
    set lines {}
    while {1} {
        set line [gets $fp]
        if {[eof $fp]} {
           close $fp
           break;
        }

        lappend lines $line
    }

    assert_equal [llength $lines] [llength $content]

    for { set i 0 } { $i < [llength $lines] } {incr i} {
        assert_equal [lindex $lines $i] [lindex $content $i]
    }
}

proc clean_aof_persistence {aof_dirpath} {
    catch {eval exec rm -rf [glob $aof_dirpath]}
}

proc append_to_manifest {str} {
    upvar fp fp
    puts -nonewline $fp $str
}

proc create_aof_manifest {dir aof_manifest_file code} {
    create_aof_dir $dir
    upvar fp fp
    set fp [open $aof_manifest_file w+]
    uplevel 1 $code
    close $fp
}

proc append_to_aof {str} {
    upvar fp fp
    puts -nonewline $fp $str
}

proc create_aof {dir aof_file code} {
    create_aof_dir $dir
    upvar fp fp
    set fp [open $aof_file w+]
    uplevel 1 $code
    close $fp
}

proc create_aof_dir {dir_path} {
    file mkdir $dir_path
}

proc start_server_aof {overrides code} {
    upvar defaults defaults srv srv server_path server_path aof_basename aof_basename aof_dirpath aof_dirpath aof_manifest_file aof_manifest_file aof_manifest_file2 aof_manifest_file2
    set config [concat $defaults $overrides]
    start_server [list overrides $config keep_persistence true] $code
}

proc start_server_aof_ex {overrides options code} {
    upvar defaults defaults srv srv server_path server_path
    set config [concat $defaults $overrides]
    start_server [concat [list overrides $config keep_persistence true] $options] $code
}



set ::global_overrides {}
set ::tags {}
set ::valgrind_errors {}

proc start_server_error {config_file error} {
    set err {}
    append err "Can't start the Valkey server\n"
    append err "CONFIGURATION:\n"
    append err [exec cat $config_file]
    append err "\nERROR:\n"
    append err [string trim $error]
    send_data_packet $::test_server_fd err $err
}

proc check_valgrind_errors stderr {
    set res [find_valgrind_errors $stderr true]
    if {$res != ""} {
        send_data_packet $::test_server_fd err "Valgrind error: $res\n"
    }
}

proc check_sanitizer_errors stderr {
    set res [sanitizer_errors_from_file $stderr]
    if {$res != ""} {
        send_data_packet $::test_server_fd err "Sanitizer error: $res\n"
    }
}

proc clean_persistence config {
    # we may wanna keep the logs for later, but let's clean the persistence
    # files right away, since they can accumulate and take up a lot of space
    set config [dict get $config "config"]
    set dir [dict get $config "dir"]
    set rdb [format "%s/%s" $dir "dump.rdb"]
    if {[dict exists $config "appenddirname"]} {
        set aofdir [dict get $config "appenddirname"]
    } else {
        set aofdir "appendonlydir"
    }
    set aof_dirpath [format "%s/%s" $dir $aofdir]
    clean_aof_persistence $aof_dirpath
    catch {exec rm -rf $rdb}
}

proc kill_server config {
    # nothing to kill when running against external server
    if {$::external} return

    # Close client connection if exists
    if {[dict exists $config "client"]} {
        [dict get $config "client"] close
    }

    # nevermind if its already dead
    set pid [dict get $config pid]
    if {![is_alive $pid]} {
        # Check valgrind errors if needed
        if {$::valgrind} {
            check_valgrind_errors [dict get $config stderr]
        }

        check_sanitizer_errors [dict get $config stderr]

        # Remove this pid from the set of active pids in the test server.
        send_data_packet $::test_server_fd server-killed $pid

        return
    }

    # check for leaks
    if {![dict exists $config "skipleaks"]} {
        catch {
            if {[string match {*Darwin*} [exec uname -a]]} {
                tags {"leaks"} {
                    test "Check for memory leaks (pid $pid)" {
                        set output {0 leaks}
                        catch {exec leaks $pid} output option
                        # In a few tests we kill the server process, so leaks will not find it.
                        # It'll exits with exit code >1 on error, so we ignore these.
                        if {[dict exists $option -errorcode]} {
                            set details [dict get $option -errorcode]
                            if {[lindex $details 0] eq "CHILDSTATUS"} {
                                  set status [lindex $details 2]
                                  if {$status > 1} {
                                      set output "0 leaks"
                                  }
                            }
                        }
                        set output
                    } {*0 leaks*}
                }
            }
        }
    }

    # kill server and wait for the process to be totally exited
    send_data_packet $::test_server_fd server-killing $pid
    catch {exec kill $pid}
    # Node might have been stopped in the test
    catch {exec kill -SIGCONT $pid}
    if {$::valgrind} {
        set max_wait 120000
    } else {
        set max_wait 10000
    }
    while {[is_alive $pid]} {
        incr wait 10

        if {$wait == $max_wait} {
            puts "Forcing process $pid to crash..."
            catch {exec kill -SEGV $pid}
        } elseif {$wait >= $max_wait * 2} {
            puts "Forcing process $pid to exit..."
            catch {exec kill -KILL $pid}
        } elseif {$wait % 1000 == 0} {
            puts "Waiting for process $pid to exit..."
        }
        after 10
    }

    # Check valgrind errors if needed
    if {$::valgrind} {
        check_valgrind_errors [dict get $config stderr]
    }

    check_sanitizer_errors [dict get $config stderr]

    # Remove this pid from the set of active pids in the test server.
    send_data_packet $::test_server_fd server-killed $pid
}

proc is_alive pid {
    if {[catch {exec kill -0 $pid} err]} {
        return 0
    } else {
        return 1
    }
}

proc ping_server {host port} {
    set retval 0
    if {[catch {
        if {$::tls} {
            set fd [::tls::socket $host $port] 
        } else {
            set fd [socket $host $port]
        }
        fconfigure $fd -translation binary
        puts $fd "PING\r\n"
        flush $fd
        set reply [gets $fd]
        if {[string range $reply 0 0] eq {+} ||
            [string range $reply 0 0] eq {-}} {
            set retval 1
        }
        close $fd
    } e]} {
        if {$::verbose} {
            puts -nonewline "."
        }
    } else {
        if {$::verbose} {
            puts -nonewline "ok"
        }
    }
    return $retval
}

# Return 1 if the server at the specified addr is reachable by PING, otherwise
# returns 0. Performs a try every 50 milliseconds for the specified number
# of retries.
proc server_is_up {host port retrynum} {
    after 10 ;# Use a small delay to make likely a first-try success.
    set retval 0
    while {[incr retrynum -1]} {
        if {[catch {ping_server $host $port} ping]} {
            set ping 0
        }
        if {$ping} {return 1}
        after 50
    }
    return 0
}

# Check if current ::tags match requested tags. If ::allowtags are used,
# there must be some intersection. If ::denytags are used, no intersection
# is allowed. Returns 1 if tags are acceptable or 0 otherwise, in which
# case err_return names a return variable for the message to be logged.
proc tags_acceptable {tags err_return} {
    upvar $err_return err

    # If tags are whitelisted, make sure there's match
    if {[llength $::allowtags] > 0} {
        set matched 0
        foreach tag $::allowtags {
            if {[lsearch $tags $tag] >= 0} {
                incr matched
            }
        }
        if {$matched < 1} {
            set err "Tag: none of the tags allowed"
            return 0
        }
    }

    foreach tag $::denytags {
        if {[lsearch $tags $tag] >= 0} {
            set err "Tag: $tag denied"
            return 0
        }
    }

    # some units mess with the client output buffer so we can't really use the req-res logging mechanism.
    if {$::log_req_res && [lsearch $tags "logreqres:skip"] >= 0} {
        set err "Not supported when running in log-req-res mode"
        return 0
    }

    if {$::external && [lsearch $tags "external:skip"] >= 0} {
        set err "Not supported on external server"
        return 0
    }

    if {$::singledb && [lsearch $tags "singledb:skip"] >= 0} {
        set err "Not supported on singledb"
        return 0
    }

    if {$::cluster_mode && [lsearch $tags "cluster:skip"] >= 0} {
        set err "Not supported in cluster mode"
        return 0
    }

    if {$::tls && [lsearch $tags "tls:skip"] >= 0} {
        set err "Not supported in tls mode"
        return 0
    }

    if {!$::large_memory && [lsearch $tags "large-memory"] >= 0} {
        set err "large memory flag not provided"
        return 0
    }

    if {$::io_threads && [lsearch $tags "io-threads:skip"] >= 0} {
        set err "Not supported in io-threads mode"
        return 0
    }

    if {$::tcl_version < 8.6 && [lsearch $tags "ipv6"] >= 0} {
        set err "TCL version is too low and does not support this"
        return 0
    }

    return 1
}

# doesn't really belong here, but highly coupled to code in start_server
proc tags {tags code} {
    # If we 'tags' contain multiple tags, quoted and separated by spaces,
    # we want to get rid of the quotes in order to have a proper list
    set tags [string map { \" "" } $tags]
    set ::tags [concat $::tags $tags]
    if {![tags_acceptable $::tags err]} {
        incr ::num_aborted
        send_data_packet $::test_server_fd ignore $err
        set ::tags [lrange $::tags 0 end-[llength $tags]]
        return
    }
    uplevel 1 $code
    set ::tags [lrange $::tags 0 end-[llength $tags]]
}

# Write the configuration in the dictionary 'config' in the specified
# file name.
proc create_server_config_file {filename config config_lines} {
    set fp [open $filename w+]
    foreach directive [dict keys $config] {
        puts -nonewline $fp "$directive "
        puts $fp [dict get $config $directive]
    }
    foreach {config_line_directive config_line_args} $config_lines {
        puts $fp "$config_line_directive $config_line_args"
    }
    close $fp
}

proc spawn_server {config_file stdout stderr args} {
    set cmd [list src/valkey-server $config_file]
    set args {*}$args
    if {[llength $args] > 0} {
        lappend cmd {*}$args
    }

    if {$::valgrind} {
        set pid [exec valgrind --track-origins=yes --trace-children=yes --suppressions=[pwd]/src/valgrind.sup --show-reachable=no --show-possibly-lost=no --leak-check=full {*}$cmd >> $stdout 2>> $stderr &]
    } elseif ($::stack_logging) {
        set pid [exec /usr/bin/env MallocStackLogging=1 MallocLogFile=/tmp/malloc_log.txt {*}$cmd >> $stdout 2>> $stderr &]
    } else {
        # ASAN_OPTIONS environment variable is for address sanitizer. If a test
        # tries to allocate huge memory area and expects allocator to return
        # NULL, address sanitizer throws an error without this setting.
        set pid [exec /usr/bin/env ASAN_OPTIONS=allocator_may_return_null=1 {*}$cmd >> $stdout 2>> $stderr &]
    }

    if {$::wait_server} {
        set msg "server started PID: $pid. press any key to continue..."
        puts $msg
        read stdin 1
    }

    # Tell the test server about this new instance.
    send_data_packet $::test_server_fd server-spawned $pid
    return $pid
}

# Wait for actual startup, return 1 if port is busy, 0 otherwise
proc wait_server_started {config_file stdout stderr pid} {
    set checkperiod 100; # Milliseconds
    set maxiter [expr {120*1000/$checkperiod}] ; # Wait up to 2 minutes.
    set port_busy 0
    while 1 {
        if {[regexp -- " PID: $pid.*Server initialized" [exec cat $stdout]]} {
            break
        }
        after $checkperiod
        incr maxiter -1
        if {$maxiter == 0} {
            start_server_error $config_file "No PID detected in log $stdout"
            puts "--- LOG CONTENT ---"
            puts [exec cat $stdout]
            puts "-------------------"
            break
        }

        # Check if the port is actually busy and the server failed
        # for this reason.
        if {[regexp {Failed listening on port} [exec cat $stdout]]} {
            set port_busy 1
            break
        }

        # Configuration errors are unexpected, but it's helpful to fail fast
        # to give the feedback to the test runner.
        if {[regexp {FATAL CONFIG FILE ERROR} [exec cat $stderr]]} {
            start_server_error $config_file "Configuration issue prevented Valkey startup"
            break
        }
    }
    return $port_busy
}

proc dump_server_log {srv} {
    set pid [dict get $srv "pid"]
    puts "\n===== Start of server log (pid $pid) =====\n"
    puts [exec cat [dict get $srv "stdout"]]
    puts "===== End of server log (pid $pid) =====\n"

    puts "\n===== Start of server stderr log (pid $pid) =====\n"
    puts [exec cat [dict get $srv "stderr"]]
    puts "===== End of server stderr log (pid $pid) =====\n"
}

proc run_external_server_test {code overrides} {
    set srv {}
    dict set srv "host" $::host
    dict set srv "port" $::port
    set client [valkey $::host $::port 0 $::tls]
    dict set srv "client" $client
    if {!$::singledb} {
        $client select 9
    }

    set config {}
    dict set config "port" $::port
    dict set srv "config" $config

    # append the server to the stack
    lappend ::servers $srv

    if {[llength $::servers] > 1} {
        if {$::verbose} {
            puts "Notice: nested start_server statements in external server mode, test must be aware of that!"
        }
    }

    r flushall
    r function flush

    # store overrides
    set saved_config {}
    foreach {param val} $overrides {
        dict set saved_config $param [lindex [r config get $param] 1]
        r config set $param $val

        # If we enable appendonly, wait for for rewrite to complete. This is
        # required for tests that begin with a bg* command which will fail if
        # the rewriteaof operation is not completed at this point.
        if {$param == "appendonly" && $val == "yes"} {
            waitForBgrewriteaof r
        }
    }

    if {[catch {set retval [uplevel 2 $code]} error]} {
        if {$::durable} {
            set msg [string range $error 10 end]
            lappend details $msg
            lappend details $::errorInfo
            lappend ::tests_failed $details

            incr ::num_failed
            send_data_packet $::test_server_fd err [join $details "\n"]
        } else {
            # Re-raise, let handler up the stack take care of this.
            error $error $::errorInfo
        }
    }

    # restore overrides
    dict for {param val} $saved_config {
        r config set $param $val
    }

    set srv [lpop ::servers]
    
    if {[dict exists $srv "client"]} {
        [dict get $srv "client"] close
    }
}

proc start_server {options {code undefined}} {
    # setup defaults
    set baseconfig "default.conf"
    set overrides {}
    set omit {}
    set tags {}
    set args {}
    set keep_persistence false
    set config_lines {}

    # Wait for the server to be ready and check for server liveness/client connectivity before starting the test.
    set wait_ready true

    # parse options
    foreach {option value} $options {
        switch $option {
            "config" {
                set baseconfig $value
            }
            "overrides" {
                set overrides [concat $overrides $value]
            }
            "config_lines" {
                set config_lines $value
            }
            "args" {
                set args $value
            }
            "omit" {
                set omit $value
            }
            "tags" {
                # If we 'tags' contain multiple tags, quoted and separated by spaces,
                # we want to get rid of the quotes in order to have a proper list
                set tags [string map { \" "" } $value]
                set ::tags [concat $::tags $tags]
            }
            "keep_persistence" {
                set keep_persistence $value
            }
            "wait_ready" {
                set wait_ready $value
            }
            default {
                error "Unknown option $option"
            }
        }
    }

    # We skip unwanted tags
    if {![tags_acceptable $::tags err]} {
        incr ::num_aborted
        send_data_packet $::test_server_fd ignore $err
        set ::tags [lrange $::tags 0 end-[llength $tags]]
        return
    }

    # If we are running against an external server, we just push the
    # host/port pair in the stack the first time
    if {$::external} {
        run_external_server_test $code $overrides

        set ::tags [lrange $::tags 0 end-[llength $tags]]
        return
    }

    set data [split [exec cat "assets/$baseconfig"] "\n"]
    set config {}
    if {$::tls} {
        if {$::tls_module} {
            lappend config_lines [list "loadmodule" [format "%s/src/valkey-tls.so" [pwd]]]
        }
        dict set config "tls-cert-file" [format "%s/tls/server.crt" [pwd]]
        dict set config "tls-key-file" [format "%s/tls/server.key" [pwd]]
        dict set config "tls-client-cert-file" [format "%s/tls/client.crt" [pwd]]
        dict set config "tls-client-key-file" [format "%s/tls/client.key" [pwd]]
        dict set config "tls-dh-params-file" [format "%s/tls/valkey.dh" [pwd]]
        dict set config "tls-ca-cert-file" [format "%s/tls/ca.crt" [pwd]]
        dict set config "loglevel" "debug"
    }

    if {$::io_threads} {
        dict set config "io-threads" 2
        dict set config "events-per-io-thread" 0
    }

    foreach line $data {
        if {[string length $line] > 0 && [string index $line 0] ne "#"} {
            set elements [split $line " "]
            set directive [lrange $elements 0 0]
            set arguments [lrange $elements 1 end]
            dict set config $directive $arguments
        }
    }

    # use a different directory every time a server is started
    dict set config dir [tmpdir server]

    # start every server on a different port
    set port [find_available_port $::baseport $::portcount]
    if {$::tls} {
        set pport [find_available_port $::baseport $::portcount]
        dict set config "port" $pport
        dict set config "tls-port" $port
        dict set config "tls-cluster" "yes"
        dict set config "tls-replication" "yes"
    } else {
        dict set config port $port
    }

    set unixsocket [file normalize [format "%s/%s" [dict get $config "dir"] "socket"]]
    dict set config "unixsocket" $unixsocket

    # apply overrides from global space and arguments
    foreach {directive arguments} [concat $::global_overrides $overrides] {
        dict set config $directive $arguments
    }

    # remove directives that are marked to be omitted
    foreach directive $omit {
        dict unset config $directive
    }

    if {$::log_req_res} {
        dict set config "req-res-logfile" "stdout.reqres"
    }

    if {$::force_resp3} {
        dict set config "client-default-resp" "3"
    }

    # write new configuration to temporary file
    set config_file [tmpfile valkey.conf]
    create_server_config_file $config_file $config $config_lines

    set stdout [format "%s/%s" [dict get $config "dir"] "stdout"]
    set stderr [format "%s/%s" [dict get $config "dir"] "stderr"]

    # if we're inside a test, write the test name to the server log file
    if {[info exists ::cur_test]} {
        set fd [open $stdout "a+"]
        puts $fd "### Starting server for test $::cur_test"
        close $fd
        if {$::verbose > 1} {
            puts "### Starting server $stdout for test - $::cur_test"
        }
    }

    # We may have a stdout left over from the previous tests, so we need
    # to get the current count of ready logs
    set previous_ready_count [count_message_lines $stdout "Ready to accept"]

    # We need a loop here to retry with different ports.
    set server_started 0
    while {$server_started == 0} {
        if {$::verbose} {
            puts -nonewline "=== ($tags) Starting server ${::host}:${port} "
        }

        send_data_packet $::test_server_fd "server-spawning" "port $port"

        set pid [spawn_server $config_file $stdout $stderr $args]

        # check that the server actually started
        set port_busy [wait_server_started $config_file $stdout $stderr $pid]

        # Sometimes we have to try a different port, even if we checked
        # for availability. Other test clients may grab the port before we
        # are able to do it for example.
        if {$port_busy} {
            puts "Port $port was already busy, trying another port..."
            set port [find_available_port $::baseport $::portcount]
            if {$::tls} {
                set pport [find_available_port $::baseport $::portcount]
                dict set config port $pport
                dict set config "tls-port" $port
            } else {
                dict set config port $port
            }
            create_server_config_file $config_file $config $config_lines

            # Truncate log so wait_server_started will not be looking at
            # output of the failed server.
            close [open $stdout "w"]

            continue; # Try again
        }

        if {$::valgrind} {set retrynum 1000} else {set retrynum 100}
        if {$code ne "undefined" && $wait_ready} {
            set serverisup [server_is_up $::host $port $retrynum]
        } else {
            set serverisup 1
        }

        if {$::verbose} {
            puts ""
        }

        if {!$serverisup} {
            set err {}
            append err [exec cat $stdout] "\n" [exec cat $stderr]
            start_server_error $config_file $err
            return
        }
        set server_started 1
    }

    # setup properties to be able to initialize a client object
    set port_param [expr $::tls ? {"tls-port"} : {"port"}]
    set host $::host
    if {[dict exists $config bind]} { set host [lindex [dict get $config bind] 0] }
    if {[dict exists $config $port_param]} { set port [dict get $config $port_param] }

    # setup config dict
    dict set srv "config_file" $config_file
    dict set srv "config" $config
    dict set srv "pid" $pid
    dict set srv "host" $host
    dict set srv "port" $port
    dict set srv "stdout" $stdout
    dict set srv "stderr" $stderr
    dict set srv "unixsocket" $unixsocket
    if {$::tls} {
        dict set srv "pport" $pport
    }

    # if a block of code is supplied, we wait for the server to become
    # available, create a client object and kill the server afterwards
    if {$code ne "undefined"} {
        set line [exec head -n1 $stdout]
        if {[string match {*already in use*} $line]} {
            error_and_quit $config_file $line
        }

        # append the server to the stack
        lappend ::servers $srv

        if {$wait_ready} {
            while 1 {
                # check that the server actually started and is ready for connections
                if {[count_message_lines $stdout "Ready to accept"] > $previous_ready_count} {
                    break
                }
                after 10
            }

            # connect client (after server dict is put on the stack)
            reconnect
        }

        # remember previous num_failed to catch new errors
        set prev_num_failed $::num_failed

        # execute provided block
        set num_tests $::num_tests
        if {[catch { uplevel 1 $code } error]} {
            set backtrace $::errorInfo
            set assertion [string match "assertion:*" $error]

            # fetch srv back from the server list, in case it was restarted by restart_server (new PID)
            set srv [lindex $::servers end]

            # pop the server object
            set ::servers [lrange $::servers 0 end-1]

            # Kill the server without checking for leaks
            dict set srv "skipleaks" 1
            kill_server $srv

            if {$::dump_logs} {
                # crash or assertion ($::num_failed isn't incremented yet)
                # this happens when the test spawns a server and not the other way around
                dump_server_log $srv
            } else {
                # Print crash report from log
                set crashlog [crashlog_from_file [dict get $srv "stdout"]]
                if {[string length $crashlog] > 0} {
                    puts [format "\nLogged crash report (pid %d):" [dict get $srv "pid"]]
                    puts "$crashlog"
                    puts ""
                }

                set sanitizerlog [sanitizer_errors_from_file [dict get $srv "stderr"]]
                if {[string length $sanitizerlog] > 0} {
                    puts [format "\nLogged sanitizer errors (pid %d):" [dict get $srv "pid"]]
                    puts "$sanitizerlog"
                    puts ""
                }
            }

            if {!$assertion && $::durable} {
                # durable is meant to prevent the whole tcl test from exiting on
                # an exception. an assertion will be caught by the test proc.
                set msg [string range $error 10 end]
                lappend details $msg
                lappend details $backtrace
                lappend ::tests_failed $details

                incr ::num_failed
                send_data_packet $::test_server_fd err [join $details "\n"]
            } else {
                # Re-raise, let handler up the stack take care of this.
                error $error $backtrace
            }
        } else {
            if {$::dump_logs && $prev_num_failed != $::num_failed} {
                dump_server_log $srv
            }
        }

        # fetch srv back from the server list, in case it was restarted by restart_server (new PID)
        set srv [lindex $::servers end]

        # Don't do the leak check when no tests were run
        if {$num_tests == $::num_tests} {
            dict set srv "skipleaks" 1
        }

        # pop the server object
        set ::servers [lrange $::servers 0 end-1]

        set ::tags [lrange $::tags 0 end-[llength $tags]]
        kill_server $srv
        if {!$keep_persistence} {
            clean_persistence $srv
        }
        set _ ""
    } else {
        set ::tags [lrange $::tags 0 end-[llength $tags]]
        set _ $srv
    }
}

# Start multiple servers with the same options, run code, then stop them.
proc start_multiple_servers {num options code} {
    for {set i 0} {$i < $num} {incr i} {
        set code [list start_server $options $code]
    }
    uplevel 1 $code
}

proc restart_server {level wait_ready rotate_logs {reconnect 1} {shutdown sigterm}} {
    set srv [lindex $::servers end+$level]
    if {$shutdown ne {sigterm}} {
        catch {[dict get $srv "client"] shutdown $shutdown}
    }
    # Kill server doesn't mind if the server is already dead
    kill_server $srv
    # Remove the default client from the server
    dict unset srv "client"

    set pid [dict get $srv "pid"]
    set stdout [dict get $srv "stdout"]
    set stderr [dict get $srv "stderr"]
    if {$rotate_logs} {
        set ts [clock format [clock seconds] -format %y%m%d%H%M%S]
        file rename $stdout $stdout.$ts.$pid
        file rename $stderr $stderr.$ts.$pid
    }
    set prev_ready_count [count_message_lines $stdout "Ready to accept"]

    # if we're inside a test, write the test name to the server log file
    if {[info exists ::cur_test]} {
        set fd [open $stdout "a+"]
        puts $fd "### Restarting server for test $::cur_test"
        close $fd
    }

    set config_file [dict get $srv "config_file"]

    set pid [spawn_server $config_file $stdout $stderr {}]

    # check that the server actually started
    wait_server_started $config_file $stdout $stderr $pid

    # update the pid in the servers list
    dict set srv "pid" $pid
    # re-set $srv in the servers list
    lset ::servers end+$level $srv

    if {$wait_ready} {
        while 1 {
            # check that the server actually started and is ready for connections
            if {[count_message_lines $stdout "Ready to accept"] > $prev_ready_count} {
                break
            }
            after 10
        }
    }
    if {$reconnect} {
        reconnect $level
    }
}

# Cluster helper functions

proc valkeycli_tls_config {testsdir} {
    set tlsdir [file join $testsdir tls]
    set cert [file join $tlsdir client.crt]
    set key [file join $tlsdir client.key]
    set cacert [file join $tlsdir ca.crt]

    if {$::tls} {
        return [list --tls --cert $cert --key $key --cacert $cacert]
    } else {
        return {}
    }
}

# Returns command line for executing valkey-cli
proc valkeycli {host port {opts {}}} {
    set cmd [list src/valkey-cli -h $host -p $port]
    lappend cmd {*}[valkeycli_tls_config "tests"]
    lappend cmd {*}$opts
    return $cmd
}

proc valkeycliuri {scheme host port {opts {}}} {
    set cmd [list src/valkey-cli -u $scheme$host:$port]
    lappend cmd {*}[valkeycli_tls_config "tests"]
    lappend cmd {*}$opts
    return $cmd
}

# Returns command line for executing valkey-cli with a unix socket address
proc valkeycli_unixsocket {unixsocket {opts {}}} {
    return [list src/valkey-cli -s $unixsocket {*}$opts]
}

# Run valkey-cli with specified args on the server of specified level.
# Returns output broken down into individual lines.
proc valkeycli_exec {level args} {
    set cmd [valkeycli_unixsocket [srv $level unixsocket] $args]
    set fd [open "|$cmd" "r"]
    set ret [lrange [split [read $fd] "\n"] 0 end-1]
    close $fd

    return $ret
}

# Tcl cluster client as a wrapper of redis.rb.
# Copyright (c) 2014 Redis Ltd.
# Released under the BSD license like Redis itself
#
# Example usage:
#
# set c [valkey_cluster {127.0.0.1:6379 127.0.0.1:6380}]
# $c set foo
# $c get foo
# $c close

package provide valkey_cluster 0.1

namespace eval valkey_cluster {}
set ::valkey_cluster::internal_id 0
set ::valkey_cluster::id 0
array set ::valkey_cluster::startup_nodes {}
array set ::valkey_cluster::nodes {}
array set ::valkey_cluster::slots {}
array set ::valkey_cluster::tls {}

# List of "plain" commands, which are commands where the sole key is always
# the first argument.
set ::valkey_cluster::plain_commands {
    get set setnx setex psetex append strlen exists setbit getbit
    setrange getrange substr incr decr rpush lpush rpushx lpushx
    linsert rpop lpop brpop llen lindex lset lrange ltrim lrem
    sadd srem sismember smismember scard spop srandmember smembers sscan zadd
    zincrby zrem zremrangebyscore zremrangebyrank zremrangebylex zrange
    zrangebyscore zrevrangebyscore zrangebylex zrevrangebylex zcount
    zlexcount zrevrange zcard zscore zmscore zrank zrevrank zscan hset hsetnx
    hget hmset hmget hincrby hincrbyfloat hdel hlen hkeys hvals
    hgetall hexists hscan incrby decrby incrbyfloat getset move
    expire expireat pexpire pexpireat type ttl pttl persist restore
    dump bitcount bitpos pfadd pfcount cluster ssubscribe spublish
    sunsubscribe
}

# Create a cluster client. The nodes are given as a list of host:port. The TLS
# parameter (1 or 0) is optional and defaults to the global $::tls.
proc valkey_cluster {nodes {tls -1}} {
    set id [incr ::valkey_cluster::id]
    set ::valkey_cluster::startup_nodes($id) $nodes
    set ::valkey_cluster::nodes($id) {}
    set ::valkey_cluster::slots($id) {}
    set ::valkey_cluster::tls($id) [expr $tls == -1 ? $::tls : $tls]
    set handle [interp alias {} ::valkey_cluster::instance$id {} ::valkey_cluster::__dispatch__ $id]
    $handle refresh_nodes_map
    return $handle
}

# Totally reset the slots / nodes state for the client, calls
# CLUSTER NODES in the first startup node available, populates the
# list of nodes ::valkey_cluster::nodes($id) with an hash mapping node
# ip:port to a representation of the node (another hash), and finally
# maps ::valkey_cluster::slots($id) with an hash mapping slot numbers
# to node IDs.
#
# This function is called when a new Cluster client is initialized
# and every time we get a -MOVED redirection error.
proc ::valkey_cluster::__method__refresh_nodes_map {id} {
    # Contact the first responding startup node.
    set idx 0; # Index of the node that will respond.
    set errmsg {}
    foreach start_node $::valkey_cluster::startup_nodes($id) {
        set ip_port [lindex [split $start_node @] 0]
        lassign [split $ip_port :] start_host start_port
        set tls $::valkey_cluster::tls($id)
        if {[catch {
            set r {}
            set r [valkey $start_host $start_port 0 $tls]
            set nodes_descr [$r cluster nodes]
            $r close
        } e]} {
            if {$r ne {}} {catch {$r close}}
            incr idx
            if {[string length $errmsg] < 200} {
                append errmsg " $ip_port: $e"
            }
            continue ; # Try next.
        } else {
            break; # Good node found.
        }
    }

    if {$idx == [llength $::valkey_cluster::startup_nodes($id)]} {
        error "No good startup node found. $errmsg"
    }

    # Put the node that responded as first in the list if it is not
    # already the first.
    if {$idx != 0} {
        set l $::valkey_cluster::startup_nodes($id)
        set left [lrange $l 0 [expr {$idx-1}]]
        set right [lrange $l [expr {$idx+1}] end]
        set l [concat [lindex $l $idx] $left $right]
        set ::valkey_cluster::startup_nodes($id) $l
    }

    # Parse CLUSTER NODES output to populate the nodes description.
    set nodes {} ; # addr -> node description hash.
    foreach line [split $nodes_descr "\n"] {
        set line [string trim $line]
        if {$line eq {}} continue
        set args [split $line " "]
        lassign $args nodeid addr flags slaveof pingsent pongrecv configepoch linkstate
        set slots [lrange $args 8 end]
        set addr [lindex [split $addr @] 0]
        if {$addr eq {:0}} {
            set addr $start_host:$start_port
        }
        lassign [split $addr :] host port

        # Connect to the node
        set link {}
        set tls $::valkey_cluster::tls($id)
        catch {set link [valkey $host $port 0 $tls]}

        # Build this node description as an hash.
        set node [dict create \
            id $nodeid \
            internal_id $id \
            addr $addr \
            host $host \
            port $port \
            flags $flags \
            slaveof $slaveof \
            slots $slots \
            link $link \
        ]
        dict set nodes $addr $node
        lappend ::valkey_cluster::startup_nodes($id) $addr
    }

    # Close all the existing links in the old nodes map, and set the new
    # map as current.
    foreach n $::valkey_cluster::nodes($id) {
        catch {
            [dict get $n link] close
        }
    }
    set ::valkey_cluster::nodes($id) $nodes

    # Populates the slots -> nodes map.
    dict for {addr node} $nodes {
        foreach slotrange [dict get $node slots] {
            lassign [split $slotrange -] start end
            if {$end == {}} {set end $start}
            for {set j $start} {$j <= $end} {incr j} {
                dict set ::valkey_cluster::slots($id) $j $addr
            }
        }
    }

    # Only retain unique entries in the startup nodes list
    set ::valkey_cluster::startup_nodes($id) [lsort -unique $::valkey_cluster::startup_nodes($id)]
}

# Free a valkey_cluster handle.
proc ::valkey_cluster::__method__close {id} {
    catch {
        set nodes $::valkey_cluster::nodes($id)
        dict for {addr node} $nodes {
            catch {
                [dict get $node link] close
            }
        }
    }
    catch {unset ::valkey_cluster::startup_nodes($id)}
    catch {unset ::valkey_cluster::nodes($id)}
    catch {unset ::valkey_cluster::slots($id)}
    catch {unset ::valkey_cluster::tls($id)}
    catch {interp alias {} ::valkey_cluster::instance$id {}}
}

proc ::valkey_cluster::__method__masternode_for_slot {id slot} {
    # Get the node mapped to this slot.
    set node_addr [dict get $::valkey_cluster::slots($id) $slot]
    if {$node_addr eq {}} {
        error "No mapped node for slot $slot."
    }
    return [dict get $::valkey_cluster::nodes($id) $node_addr]
}

proc ::valkey_cluster::__method__masternode_notfor_slot {id slot} {
    # Get a node that is not mapped to this slot.
    set node_addr [dict get $::valkey_cluster::slots($id) $slot]
    set addrs [dict keys $::valkey_cluster::nodes($id)]
    foreach addr [lshuffle $addrs] {
        set node [dict get $::valkey_cluster::nodes($id) $addr]
        if {$node_addr ne $addr && [dict get $node slaveof] eq "-"} {
            return $node
        }
    }
    error "Slot $slot is everywhere"
}

proc ::valkey_cluster::__dispatch__ {id method args} {
    if {[info command ::valkey_cluster::__method__$method] eq {}} {
        # Get the keys from the command.
        set keys [::valkey_cluster::get_keys_from_command $method $args]
        if {$keys eq {}} {
            error "Valkey command '$method' is not supported by valkey_cluster."
        }

        # Resolve the keys in the corresponding hash slot they hash to.
        set slot [::valkey_cluster::get_slot_from_keys $keys]
        if {$slot eq {}} {
            error "Invalid command: multiple keys not hashing to the same slot."
        }

        # Get the node mapped to this slot.
        set node_addr [dict get $::valkey_cluster::slots($id) $slot]
        if {$node_addr eq {}} {
            error "No mapped node for slot $slot."
        }

        # Execute the command in the node we think is the slot owner.
        set retry 100
        set asking 0
        while {[incr retry -1]} {
            if {$retry < 5} {after 100}
            set node [dict get $::valkey_cluster::nodes($id) $node_addr]
            set link [dict get $node link]
            if {$asking} {
                $link ASKING
                set asking 0
            }
            if {[catch {$link $method {*}$args} e]} {
                if {$link eq {} || \
                    [string range $e 0 4] eq {MOVED} || \
                    [string range $e 0 2] eq {I/O} \
                } {
                    # MOVED redirection.
                    ::valkey_cluster::__method__refresh_nodes_map $id
                    set node_addr [dict get $::valkey_cluster::slots($id) $slot]
                    continue
                } elseif {[string range $e 0 2] eq {ASK}} {
                    # ASK redirection.
                    set node_addr [lindex $e 2]
                    set asking 1
                    continue
                } else {
                    # Non redirecting error.
                    error $e $::errorInfo $::errorCode
                }
            } else {
                # OK query went fine
                return $e
            }
        }
        error "Too many redirections or failures contacting Valkey Cluster."
    } else {
        uplevel 1 [list ::valkey_cluster::__method__$method $id] $args
    }
}

proc ::valkey_cluster::get_keys_from_command {cmd argv} {
    set cmd [string tolower $cmd]
    # Most commands get just one key as first argument.
    if {[lsearch -exact $::valkey_cluster::plain_commands $cmd] != -1} {
        return [list [lindex $argv 0]]
    }

    # Special handling for other commands
    switch -exact $cmd {
        mget {return $argv}
        eval {return [lrange $argv 2 1+[lindex $argv 1]]}
        evalsha {return [lrange $argv 2 1+[lindex $argv 1]]}
        spublish {return [list [lindex $argv 1]]}
    }

    # All the remaining commands are not handled.
    return {}
}

# Returns the CRC16 of the specified string.
# The CRC parameters are described in the Cluster specification.
set ::valkey_cluster::XMODEMCRC16Lookup {
    0x0000 0x1021 0x2042 0x3063 0x4084 0x50a5 0x60c6 0x70e7
    0x8108 0x9129 0xa14a 0xb16b 0xc18c 0xd1ad 0xe1ce 0xf1ef
    0x1231 0x0210 0x3273 0x2252 0x52b5 0x4294 0x72f7 0x62d6
    0x9339 0x8318 0xb37b 0xa35a 0xd3bd 0xc39c 0xf3ff 0xe3de
    0x2462 0x3443 0x0420 0x1401 0x64e6 0x74c7 0x44a4 0x5485
    0xa56a 0xb54b 0x8528 0x9509 0xe5ee 0xf5cf 0xc5ac 0xd58d
    0x3653 0x2672 0x1611 0x0630 0x76d7 0x66f6 0x5695 0x46b4
    0xb75b 0xa77a 0x9719 0x8738 0xf7df 0xe7fe 0xd79d 0xc7bc
    0x48c4 0x58e5 0x6886 0x78a7 0x0840 0x1861 0x2802 0x3823
    0xc9cc 0xd9ed 0xe98e 0xf9af 0x8948 0x9969 0xa90a 0xb92b
    0x5af5 0x4ad4 0x7ab7 0x6a96 0x1a71 0x0a50 0x3a33 0x2a12
    0xdbfd 0xcbdc 0xfbbf 0xeb9e 0x9b79 0x8b58 0xbb3b 0xab1a
    0x6ca6 0x7c87 0x4ce4 0x5cc5 0x2c22 0x3c03 0x0c60 0x1c41
    0xedae 0xfd8f 0xcdec 0xddcd 0xad2a 0xbd0b 0x8d68 0x9d49
    0x7e97 0x6eb6 0x5ed5 0x4ef4 0x3e13 0x2e32 0x1e51 0x0e70
    0xff9f 0xefbe 0xdfdd 0xcffc 0xbf1b 0xaf3a 0x9f59 0x8f78
    0x9188 0x81a9 0xb1ca 0xa1eb 0xd10c 0xc12d 0xf14e 0xe16f
    0x1080 0x00a1 0x30c2 0x20e3 0x5004 0x4025 0x7046 0x6067
    0x83b9 0x9398 0xa3fb 0xb3da 0xc33d 0xd31c 0xe37f 0xf35e
    0x02b1 0x1290 0x22f3 0x32d2 0x4235 0x5214 0x6277 0x7256
    0xb5ea 0xa5cb 0x95a8 0x8589 0xf56e 0xe54f 0xd52c 0xc50d
    0x34e2 0x24c3 0x14a0 0x0481 0x7466 0x6447 0x5424 0x4405
    0xa7db 0xb7fa 0x8799 0x97b8 0xe75f 0xf77e 0xc71d 0xd73c
    0x26d3 0x36f2 0x0691 0x16b0 0x6657 0x7676 0x4615 0x5634
    0xd94c 0xc96d 0xf90e 0xe92f 0x99c8 0x89e9 0xb98a 0xa9ab
    0x5844 0x4865 0x7806 0x6827 0x18c0 0x08e1 0x3882 0x28a3
    0xcb7d 0xdb5c 0xeb3f 0xfb1e 0x8bf9 0x9bd8 0xabbb 0xbb9a
    0x4a75 0x5a54 0x6a37 0x7a16 0x0af1 0x1ad0 0x2ab3 0x3a92
    0xfd2e 0xed0f 0xdd6c 0xcd4d 0xbdaa 0xad8b 0x9de8 0x8dc9
    0x7c26 0x6c07 0x5c64 0x4c45 0x3ca2 0x2c83 0x1ce0 0x0cc1
    0xef1f 0xff3e 0xcf5d 0xdf7c 0xaf9b 0xbfba 0x8fd9 0x9ff8
    0x6e17 0x7e36 0x4e55 0x5e74 0x2e93 0x3eb2 0x0ed1 0x1ef0
}

proc ::valkey_cluster::crc16 {s} {
    set s [encoding convertto ascii $s]
    set crc 0
    foreach char [split $s {}] {
        scan $char %c byte
        set crc [expr {(($crc<<8)&0xffff) ^ [lindex $::valkey_cluster::XMODEMCRC16Lookup [expr {(($crc>>8)^$byte) & 0xff}]]}]
    }
    return $crc
}

# Hash a single key returning the slot it belongs to, Implemented hash
# tags as described in the Cluster specification.
proc ::valkey_cluster::hash {key} {
    set keylen [string length $key]
    set s {}
    set e {}
    for {set s 0} {$s < $keylen} {incr s} {
        if {[string index $key $s] eq "\{"} break
    }

    if {[expr {$s == $keylen}]} {
        set res [expr {[crc16 $key] & 16383}]
        return $res
    }

    for {set e [expr {$s+1}]} {$e < $keylen} {incr e} {
        if {[string index $key $e] == "\}"} break
    }

    if {$e == $keylen || $e == [expr {$s+1}]} {
        set res [expr {[crc16 $key] & 16383}]
        return $res
    }

    set key_sub [string range $key [expr {$s+1}] [expr {$e-1}]]
    return [expr {[crc16 $key_sub] & 16383}]
}

# Return the slot the specified keys hash to.
# If the keys hash to multiple slots, an empty string is returned to
# signal that the command can't be run in Cluster.
proc ::valkey_cluster::get_slot_from_keys {keys} {
    set slot {}
    foreach k $keys {
        set s [::valkey_cluster::hash $k]
        if {$slot eq {}} {
            set slot $s
        } elseif {$slot != $s} {
            return {} ; # Error
        }
    }
    return $slot
}

proc config_set_all_nodes {keyword value} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        R $j config set $keyword $value
    }
}

proc get_instance_id_by_port {type port} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        if {[srv [expr -1*$j] port] == $port} {
            return $j
        }
    }
    fail "Instance port $port not found."
}

# Check if the cluster is writable and readable. Use node "port"
# as a starting point to talk with the cluster.
proc cluster_write_test {port} {
    set prefix [randstring 20 20 alpha]
    set cluster [valkey_cluster 127.0.0.1:$port]
    for {set j 0} {$j < 100} {incr j} {
        $cluster set key.$j $prefix.$j
    }
    for {set j 0} {$j < 100} {incr j} {
        assert {[$cluster get key.$j] eq "$prefix.$j"}
    }
    $cluster close
}

# Helper function to attempt to have each node in a cluster
# meet each other.
proc join_nodes_in_cluster {} {
    # Join node 0 with 1, 1 with 2, ... and so forth.
    # If auto-discovery works all nodes will know every other node
    # eventually.
    set ids {}
    for {set id 0} {$id < [llength $::servers]} {incr id} {lappend ids $id}
    for {set j 0} {$j < [expr [llength $ids]-1]} {incr j} {
        set a [lindex $ids $j]
        set b [lindex $ids [expr $j+1]]
        set b_port [srv -$b port]
        R $a cluster meet 127.0.0.1 $b_port
    }

    for {set id 0} {$id < [llength $::servers]} {incr id} {
        wait_for_condition 1000 50 {
            [llength [get_cluster_nodes $id connected]] == [llength $ids]
        } else {
            return 0
        }
    }
    return 1
}

# Search the first node starting from ID $first that is not
# already configured as a replica.
proc cluster_find_available_replica {first} {
    for {set id 0} {$id < [llength $::servers]} {incr id} {
        if {$id < $first} continue
        set me [cluster_get_myself $id]
        if {[dict get $me slaveof] eq {-}} {return $id}
    }
    fail "No available replicas"
}

proc fix_cluster {addr} {
    set code [catch {
        exec src/valkey-cli {*}[valkeycli_tls_config "./tests"] --cluster fix $addr << yes
    } result]
    if {$code != 0} {
        puts "valkey-cli --cluster fix returns non-zero exit code, output below:\n$result"
    }
    # Note: valkey-cli --cluster fix may return a non-zero exit code if nodes don't agree,
    # but we can ignore that and rely on the check below.
    wait_for_cluster_state ok
    wait_for_condition 100 100 {
        [catch {exec src/valkey-cli {*}[valkeycli_tls_config "./tests"] --cluster check $addr} result] == 0
    } else {
        puts "valkey-cli --cluster check returns non-zero exit code, output below:\n$result"
        fail "Cluster could not settle with configuration"
    }
}

# Check if cluster configuration is consistent.
# All the nodes in the cluster should show same slots configuration and have health
# state "online" to be considered as consistent.
proc cluster_config_consistent {} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        # Check if all the nodes are online
        set shards_cfg [R $j CLUSTER SHARDS]
        foreach shard_cfg $shards_cfg {
            set nodes [dict get $shard_cfg nodes]
            foreach node $nodes {
                if {[dict get $node health] ne "online"} {
                    return 0
                }
            }
        }

        if {$j == 0} {
            set base_cfg [R $j cluster slots]
        } else {
            if {[R $j cluster slots] != $base_cfg} {
                return 0
            }
        }
    }

    return 1
}

# Check if cluster size is consistent.
proc cluster_size_consistent {cluster_size} {
    for {set j 0} {$j < $cluster_size} {incr j} {
        if {[CI $j cluster_known_nodes] ne $cluster_size} {
            return 0
        }
    }
    return 1
}

# Wait for cluster configuration to propagate and be consistent across nodes.
proc wait_for_cluster_propagation {} {
    wait_for_condition 1000 50 {
        [cluster_config_consistent] eq 1
    } else {
        fail "cluster config did not reach a consistent state"
    }
}

# Wait for cluster size to be consistent across nodes.
proc wait_for_cluster_size {cluster_size} {
    wait_for_condition 1000 50 {
        [cluster_size_consistent $cluster_size] eq 1
    } else {
        fail "cluster size did not reach a consistent size $cluster_size"
    }
}

# Check that cluster nodes agree about "state", or raise an error.
proc wait_for_cluster_state {state} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        wait_for_condition 1000 50 {
            [CI $j cluster_state] eq $state
        } else {
            fail "Cluster node $j cluster_state:[CI $j cluster_state]"
        }
    }
}

# Default slot allocation for clusters, each master has a continuous block
# and approximately equal number of slots.
proc continuous_slot_allocation {masters replicas} {
    set avg [expr double(16384) / $masters]
    set slot_start 0
    for {set j 0} {$j < $masters} {incr j} {
        set slot_end [expr int(ceil(($j + 1) * $avg) - 1)]
        R $j cluster addslotsrange $slot_start $slot_end
        set slot_start [expr $slot_end + 1]
    }
}

# Assuming nodes are reset, this function performs slots allocation.
# Only the first 'masters' nodes are used.
proc cluster_allocate_slots {masters replicas} {
    set slot 16383
    while {$slot >= 0} {
        # Allocate successive slots to random nodes.
        set node [randomInt $masters]
        lappend slots_$node $slot
        incr slot -1
    }
    for {set j 0} {$j < $masters} {incr j} {
        R $j cluster addslots {*}[set slots_${j}]
    }
}

proc default_replica_allocation {masters replicas} {
    # Setup master/replica relationships
    set node_count [expr $masters + $replicas]
    for {set i 0} {$i < $masters} {incr i} {
        set nodeid [R $i CLUSTER MYID]
        for {set j [expr $i + $masters]} {$j < $node_count} {incr j $masters} {
            R $j CLUSTER REPLICATE $nodeid
        }
    }
}

# Add 'replicas' replicas to a cluster composed of 'masters' masters.
# It assumes that masters are allocated sequentially from instance ID 0
# to N-1.
proc cluster_allocate_replicas {masters replicas} {
    for {set j 0} {$j < $replicas} {incr j} {
        set master_id [expr {$j % $masters}]
        set replica_id [cluster_find_available_replica $masters]
        set master_myself [cluster_get_myself $master_id]
        R $replica_id cluster replicate [dict get $master_myself id]
    }
}

# Setup method to be executed to configure the cluster before the
# tests run.
proc cluster_setup {masters replicas node_count slot_allocator replica_allocator code} {
    # Have all nodes meet
    if {$::tls} {
        set tls_cluster [lindex [R 0 CONFIG GET tls-cluster] 1]
    }
    if {$::tls && !$tls_cluster} {
        for {set i 1} {$i < $node_count} {incr i} {
            R 0 CLUSTER MEET [srv -$i host] [srv -$i pport]
        }         
    } else {
        for {set i 1} {$i < $node_count} {incr i} {
            R 0 CLUSTER MEET [srv -$i host] [srv -$i port]
        }
    }  

    $slot_allocator $masters $replicas

    wait_for_cluster_propagation

    # Setup master/replica relationships
    $replica_allocator $masters $replicas

    # A helper debug log that can print the server id in the server logs.
    # This can help us locate the corresponding server in the log file.
    for {set i 0} {$i < $masters} {incr i} {
        R $i DEBUG LOG "========== I am primary $i =========="
    }
    for {set i $i} {$i < [expr $masters+$replicas]} {incr i} {
        R $i DEBUG LOG "========== I am replica $i =========="
    }

    wait_for_cluster_propagation
    wait_for_cluster_state "ok"

    uplevel 1 $code
}

# Start a cluster with the given number of masters and replicas. Replicas
# will be allocated to masters by round robin.
proc start_cluster {masters replicas options code {slot_allocator continuous_slot_allocation} {replica_allocator default_replica_allocation}} {
    set node_count [expr $masters + $replicas]

    # Set the final code to be the tests + cluster setup
    set code [list cluster_setup $masters $replicas $node_count $slot_allocator $replica_allocator $code]

    # Configure the starting of multiple servers. Set cluster node timeout
    # aggressively since many tests depend on ping/pong messages. 
    set cluster_options [list overrides [list cluster-enabled yes cluster-ping-interval 100 cluster-node-timeout 3000]]
    set options [concat $cluster_options $options]

    # Cluster mode only supports a single database, so before executing the tests
    # it needs to be configured correctly and needs to be reset after the tests. 
    set old_singledb $::singledb
    set ::singledb 1
    start_multiple_servers $node_count $options $code
    set ::singledb $old_singledb
}

# Test node for flag.
proc cluster_has_flag {node flag} {
    expr {[lsearch -exact [dict get $node flags] $flag] != -1}
}

# Returns the parsed "myself" node entry as a dictionary.
proc cluster_get_myself id {
    set nodes [get_cluster_nodes $id]
    foreach n $nodes {
        if {[cluster_has_flag $n myself]} {return $n}
    }
    return {}
}

# Returns the parsed "myself's primary" CLUSTER NODES entry as a dictionary.
proc cluster_get_myself_primary id {
    set myself [cluster_get_myself $id]
    set replicaof [dict get $myself slaveof]
    set node [cluster_get_node_by_id $id $replicaof]
    return $node
}

# Get a specific node by ID by parsing the CLUSTER NODES output
# of the instance Number 'instance_id'
proc cluster_get_node_by_id {instance_id node_id} {
    set nodes [get_cluster_nodes $instance_id]
    foreach n $nodes {
        if {[dict get $n id] eq $node_id} {return $n}
    }
    return {}
}

# Returns a parsed CLUSTER NODES output as a list of dictionaries. Optional status field
# can be specified to only returns entries that match the provided status.
proc get_cluster_nodes {id {status "*"}} {
    set lines [split [R $id cluster nodes] "\r\n"]
    set nodes {}
    foreach l $lines {
        set l [string trim $l]
        if {$l eq {}} continue
        set args [split $l]
        set node [dict create \
            id [lindex $args 0] \
            addr [lindex $args 1] \
            flags [split [lindex $args 2] ,] \
            slaveof [lindex $args 3] \
            ping_sent [lindex $args 4] \
            pong_recv [lindex $args 5] \
            config_epoch [lindex $args 6] \
            linkstate [lindex $args 7] \
            slots [lrange $args 8 end] \
        ]
        if {[string match $status [lindex $args 7]]} {
            lappend nodes $node
        }
    }
    return $nodes
}

# Returns 1 if no node knows node_id, 0 if any node knows it.
proc node_is_forgotten {node_id} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        set cluster_nodes [R $j CLUSTER NODES]
        if { [string match "*$node_id*" $cluster_nodes] } {
            return 0
        }
    }
    return 1
}

# Isolate a node from the cluster and give it a new nodeid
proc isolate_node {id} {
    set node_id [R $id CLUSTER MYID]
    R $id CLUSTER RESET HARD
    # Here we additionally test that CLUSTER FORGET propagates to all nodes.
    set other_id [expr $id == 0 ? 1 : 0]
    R $other_id CLUSTER FORGET $node_id
    wait_for_condition 50 100 {
        [node_is_forgotten $node_id]
    } else {
        fail "CLUSTER FORGET was not propagated to all nodes"
    }
}

# Check if cluster's view of hostnames is consistent
proc are_hostnames_propagated {match_string} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        set cfg [R $j cluster slots]
        foreach node $cfg {
            for {set i 2} {$i < [llength $node]} {incr i} {
                if {! [string match $match_string [lindex [lindex [lindex $node $i] 3] 1]] } {
                    return 0
                }
            }
        }
    }
    return 1
}

# Check if cluster's announced IPs are consistent and match a pattern
# Optionally, a list of clients can be supplied.
proc are_cluster_announced_ips_propagated {match_string {clients {}}} {
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        if {$clients eq {}} {
            set client [srv [expr -1*$j] "client"]
        } else {
            set client [lindex $clients $j]
        }
        set cfg [$client cluster slots]
        foreach node $cfg {
            for {set i 2} {$i < [llength $node]} {incr i} {
                if {! [string match $match_string [lindex [lindex $node $i] 0]] } {
                    return 0
                }
            }
        }
    }
    return 1
}

proc wait_node_marked_fail {ref_node_index instance_id_to_check} {
    wait_for_condition 1000 50 {
        [check_cluster_node_mark fail $ref_node_index $instance_id_to_check]
    } else {
        fail "Replica node never marked as FAIL ('fail')"
    }
}

proc wait_node_marked_pfail {ref_node_index instance_id_to_check} {
    wait_for_condition 1000 50 {
        [check_cluster_node_mark fail\? $ref_node_index $instance_id_to_check]
    } else {
        fail "Replica node never marked as PFAIL ('fail?')"
    }
}

proc check_cluster_node_mark {flag ref_node_index instance_id_to_check} {
    set nodes [get_cluster_nodes $ref_node_index]

    foreach n $nodes {
        if {[dict get $n id] eq $instance_id_to_check} {
            return [cluster_has_flag $n $flag]
        }
    }
    fail "Unable to find instance id in cluster nodes. ID: $instance_id_to_check"
}

proc get_slot_field {slot_output shard_id node_id attrib_id} {
    return [lindex [lindex [lindex $slot_output $shard_id] $node_id] $attrib_id]
}

set ::tmpcounter 0
set ::tmproot "./tmp"
file mkdir $::tmproot

# returns a dirname unique to this process to write to
proc tmpdir {basename} {
    set dir [file join $::tmproot $basename.[pid].[incr ::tmpcounter]]
    file mkdir $dir
    set _ $dir
}

# return a filename unique to this process to write to
proc tmpfile {basename} {
    file join $::tmproot $basename.[pid].[incr ::tmpcounter]
}

set ::num_tests 0
set ::num_passed 0
set ::num_failed 0
set ::num_skipped 0
set ::num_aborted 0
set ::tests_failed {}
set ::cur_test ""

proc fail {msg} {
    error "assertion:$msg"
}

proc assert {condition} {
    if {![uplevel 1 [list expr $condition]]} {
        set context "(context: [info frame -1])"
        error "assertion:Expected [uplevel 1 [list subst -nocommands $condition]] $context"
    }
}

proc assert_no_match {pattern value} {
    if {[string match $pattern $value]} {
        set context "(context: [info frame -1])"
        error "assertion:Expected '$value' to not match '$pattern' $context"
    }
}

proc assert_match {pattern value {detail ""} {context ""}} {
    if {![string match $pattern $value]} {
        if {$context eq ""} {
            set context "(context: [info frame -1])"
        }
        error "assertion:Expected '$value' to match '$pattern' $context $detail"
    }
}

proc assert_failed {expected_err detail} {
     if {$detail ne ""} {
        set detail "(detail: $detail)"
     } else {
        set detail "(context: [info frame -2])"
     }
     error "assertion:$expected_err $detail"
}

proc assert_not_equal {value expected {detail ""}} {
    if {!($expected ne $value)} {
        assert_failed "Expected '$value' not equal to '$expected'" $detail
    }
}

proc assert_equal {value expected {detail ""}} {
    if {$expected ne $value} {
        assert_failed "Expected '$value' to be equal to '$expected'" $detail
    }
}

proc assert_lessthan {value expected {detail ""}} {
    if {!($value < $expected)} {
        assert_failed "Expected '$value' to be less than '$expected'" $detail
    }
}

proc assert_lessthan_equal {value expected {detail ""}} {
    if {!($value <= $expected)} {
        assert_failed "Expected '$value' to be less than or equal to '$expected'" $detail
    }
}

proc assert_morethan {value expected {detail ""}} {
    if {!($value > $expected)} {
        assert_failed "Expected '$value' to be more than '$expected'" $detail
    }
}

proc assert_morethan_equal {value expected {detail ""}} {
    if {!($value >= $expected)} {
        assert_failed "Expected '$value' to be more than or equal to '$expected'" $detail
    }
}

proc assert_range {value min max {detail ""}} {
    if {!($value <= $max && $value >= $min)} {
        assert_failed "Expected '$value' to be between to '$min' and '$max'" $detail
    }
}

proc assert_error {pattern code {detail ""}} {
    if {[catch {uplevel 1 $code} error]} {
        assert_match $pattern $error $detail
    } else {
        assert_failed "Expected an error matching '$pattern' but got '$error'" $detail
    }
}

proc assert_encoding {enc key} {
    if {$::ignoreencoding} {
        return
    }
    set val [r object encoding $key]
    assert_match $enc $val
}

proc assert_type {type key} {
    assert_equal $type [r type $key]
}

proc assert_refcount {ref key} {
    if {[lsearch $::denytags "needs:debug"] >= 0} {
        return
    }

    set val [r object refcount $key]
    assert_equal $ref $val
}

proc assert_refcount_morethan {key ref} {
    if {[lsearch $::denytags "needs:debug"] >= 0} {
        return
    }

    set val [r object refcount $key]
    assert_morethan $val $ref
}

# Wait for the specified condition to be true, with the specified number of
# max retries and delay between retries. Otherwise the 'elsescript' is
# executed.
proc wait_for_condition {maxtries delay e _else_ elsescript} {
    while {[incr maxtries -1] >= 0} {
        set errcode [catch {uplevel 1 [list expr $e]} result]
        if {$errcode == 0} {
            if {$result} break
        } else {
            return -code $errcode $result
        }
        after $delay
    }
    if {$maxtries == -1} {
        set errcode [catch [uplevel 1 $elsescript] result]
        return -code $errcode $result
    }
}

proc verify_replica_online {master replica_idx max_retry} {
    set pause 100
    set count_down $max_retry
    while {$count_down} {
        set info [$master info]
        set pattern *slave$replica_idx:*state=online*
        if {[string match $pattern $info]} {
            break
        } else {
            incr count_down -1
            after $pause
        }
    }
    if {$count_down == 0} {
        set threshold [expr {$max_retry*$pause/1000}]
        error "assertion:Replica is not in sync after $threshold seconds"
    } 
}

proc wait_for_value_to_propegate_to_replica {master replica key} {
    set val [$master get $key]
    wait_for_condition 50 500 {
                ([$replica get $key] eq $val)
    } else {
        error "Key $key did not propegate. Expected $val but got [$replica get $key]"
    }
}

# try to match a value to a list of patterns that are either regex (starts with "/") or plain string.
# The caller can specify to use only glob-pattern match
proc search_pattern_list {value pattern_list {glob_pattern false}} {
    foreach el $pattern_list {
        if {[string length $el] == 0} { continue }
        if { $glob_pattern } {
            if {[string match $el $value]} {
                return 1
            }
            continue
        }
        if {[string equal / [string index $el 0]] && [regexp -- [string range $el 1 end] $value]} {
            return 1
        } elseif {[string equal $el $value]} {
            return 1
        }
    }
    return 0
}

proc test {name code {okpattern undefined} {tags {}}} {
    # abort if test name in skiptests
    if {[search_pattern_list $name $::skiptests]} {
        incr ::num_skipped
        send_data_packet $::test_server_fd skip $name
        return
    }
    if {$::verbose > 1} {
        puts "starting test $name"
    }
    # abort if only_tests was set but test name is not included
    if {[llength $::only_tests] > 0 && ![search_pattern_list $name $::only_tests]} {
        incr ::num_skipped
        send_data_packet $::test_server_fd skip $name
        return
    }

    set tags [concat $::tags $tags]
    if {![tags_acceptable $tags err]} {
        incr ::num_aborted
        send_data_packet $::test_server_fd ignore "$name: $err"
        return
    }

    incr ::num_tests
    set details {}
    lappend details "$name in $::curfile"

    # set a cur_test global to be logged into new servers that are spawn
    # and log the test name in all existing servers
    set prev_test $::cur_test
    set ::cur_test "$name in $::curfile"
    if {$::external} {
        catch {
            set r [valkey [srv 0 host] [srv 0 port] 0 $::tls]
            catch {
                $r debug log "### Starting test $::cur_test"
            }
            $r close
        }
    } else {
        set servers {}
        foreach srv $::servers {
            set stdout [dict get $srv stdout]
            set fd [open $stdout "a+"]
            puts $fd "### Starting test $::cur_test"
            close $fd
            lappend servers $stdout
        }
        if {$::verbose > 1} {
            puts "### Starting test $::cur_test - with servers: $servers"
        }
    }

    send_data_packet $::test_server_fd testing $name

    set test_start_time [clock milliseconds]
    if {[catch {set retval [uplevel 1 $code]} error]} {
        set assertion [string match "assertion:*" $error]
        if {$assertion || $::durable} {
            # durable prevents the whole tcl test from exiting on an exception.
            # an assertion is handled gracefully anyway.
            set msg [string range $error 10 end]
            lappend details $msg
            if {!$assertion} {
                lappend details $::errorInfo
            }
            lappend ::tests_failed $details

            incr ::num_failed
            send_data_packet $::test_server_fd err [join $details "\n"]

            if {$::exit_on_failure} {
                puts "Test error (last server port:[srv port], log:[srv stdout]), test will exit now"
                flush stdout
                exit 1
            }
            if {$::stop_on_failure} {
                puts "Test error (last server port:[srv port], log:[srv stdout]), press enter to teardown the test."
                flush stdout
                gets stdin
            }
        } else {
            # Re-raise, let handler up the stack take care of this.
            error $error $::errorInfo
        }
    } else {
        if {$okpattern eq "undefined" || $okpattern eq $retval || [string match $okpattern $retval]} {
            incr ::num_passed
            set elapsed [expr {[clock milliseconds]-$test_start_time}]
            send_data_packet $::test_server_fd ok $name $elapsed
        } else {
            set msg "Expected '$okpattern' to equal or match '$retval'"
            lappend details $msg
            lappend ::tests_failed $details

            incr ::num_failed
            send_data_packet $::test_server_fd err [join $details "\n"]
        }
    }

    if {$::traceleaks} {
        set output [exec leaks valkey-server]
        if {![string match {*0 leaks*} $output]} {
            send_data_packet $::test_server_fd err "Detected a memory leak in test '$name': $output"
        }
    }
    set ::cur_test $prev_test
}

proc randstring {min max {type binary}} {
    set len [expr {$min+int(rand()*($max-$min+1))}]
    set output {}
    if {$type eq {binary}} {
        set minval 0
        set maxval 255
    } elseif {$type eq {alpha} || $type eq {simplealpha}} {
        set minval 48
        set maxval 122
    } elseif {$type eq {compr}} {
        set minval 48
        set maxval 52
    }
    while {$len} {
        set num [expr {$minval+int(rand()*($maxval-$minval+1))}]
        set rr [format "%c" $num]
        if {$type eq {simplealpha} && ![string is alnum $rr]} {continue}
        if {$type eq {alpha} && $num eq 92} {continue} ;# avoid putting '\' char in the string, it can mess up TCL processing
        append output $rr
        incr len -1
    }
    return $output
}

# Useful for some test
proc zlistAlikeSort {a b} {
    if {[lindex $a 0] > [lindex $b 0]} {return 1}
    if {[lindex $a 0] < [lindex $b 0]} {return -1}
    string compare [lindex $a 1] [lindex $b 1]
}

# Return all log lines starting with the first line that contains a warning.
# Generally, this will be an assertion error with a stack trace.
proc crashlog_from_file {filename} {
    set lines [split [exec cat $filename] "\n"]
    set matched 0
    set logall 0
    set result {}
    foreach line $lines {
        if {[string match {*BUG REPORT START*} $line]} {
            set logall 1
        }
        if {[regexp {^\[\d+\]\s+\d+\s+\w+\s+\d{2}:\d{2}:\d{2} \#} $line]} {
            set matched 1
        }
        if {$logall || $matched} {
            lappend result $line
        }
    }
    join $result "\n"
}

# Return sanitizer log lines
proc sanitizer_errors_from_file {filename} {
    set log [exec cat $filename]
    set lines [split [exec cat $filename] "\n"]

    foreach line $lines {
        # Ignore huge allocation warnings
        if ([string match {*WARNING: AddressSanitizer failed to allocate*} $line]) {
            continue
        }

        # GCC UBSAN output does not contain 'Sanitizer' but 'runtime error'.
        if {[string match {*runtime error*} $line] ||
            [string match {*Sanitizer*} $line]} {
            return $log
        }
    }

    return ""
}

proc getInfoProperty {infostr property} {
    if {[regexp -lineanchor "^$property:(.*?)\r\n" $infostr _ value]} {
        return $value
    }
}

# Return value for INFO property
proc status {r property} {
    set _ [getInfoProperty [{*}$r info] $property]
}

proc waitForBgsave r {
    while 1 {
        if {[status $r rdb_bgsave_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background save to finish... "
                flush stdout
            }
            after 50
        } else {
            break
        }
    }
}

proc waitForBgrewriteaof r {
    while 1 {
        if {[status $r aof_rewrite_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background AOF rewrite to finish... "
                flush stdout
            }
            after 50
        } else {
            break
        }
    }
}

proc wait_for_sync r {
    wait_for_condition 50 100 {
        [status $r master_link_status] eq "up"
    } else {
        fail "replica didn't sync in time"
    }
}

proc wait_replica_online r {
    wait_for_condition 50 100 {
        [string match "*slave0:*,state=online*" [$r info replication]]
    } else {
        fail "replica didn't online in time"
    }
}

proc wait_for_ofs_sync {r1 r2} {
    wait_for_condition 50 100 {
        [status $r1 master_repl_offset] eq [status $r2 master_repl_offset]
    } else {
        fail "replica offset didn't match in time"
    }
}

proc wait_done_loading r {
    wait_for_condition 50 100 {
        [catch {$r ping} e] == 0
    } else {
        fail "Loading DB is taking too much time."
    }
}

proc wait_lazyfree_done r {
    wait_for_condition 50 100 {
        [status $r lazyfree_pending_objects] == 0
    } else {
        fail "lazyfree isn't done"
    }
}

# count current log lines in server's stdout
proc count_log_lines {srv_idx} {
    set _ [string trim [exec wc -l < [srv $srv_idx stdout]]]
}

# returns the number of times a line with that pattern appears in a file
proc count_message_lines {file pattern} {
    set res 0
    # exec fails when grep exists with status other than 0 (when the pattern wasn't found)
    catch {
        set res [string trim [exec grep $pattern $file 2> /dev/null | wc -l]]
    }
    return $res
}

# returns the number of times a line with that pattern appears in the log
proc count_log_message {srv_idx pattern} {
    set stdout [srv $srv_idx stdout]
    return [count_message_lines $stdout $pattern]
}

# verify pattern exists in server's stdout after a certain line number
proc verify_log_message {srv_idx pattern from_line} {
    incr from_line
    set result [exec tail -n +$from_line < [srv $srv_idx stdout]]
    if {![string match $pattern $result]} {
        fail "expected message not found in log file: $pattern"
    }
}

# verify pattern does not exists in server's stout after a certain line number
proc verify_no_log_message {srv_idx pattern from_line} {
    incr from_line
    set result [exec tail -n +$from_line < [srv $srv_idx stdout]]
    if {[string match $pattern $result]} {
        fail "expected message found in log file: $pattern"
    }
}

# wait for pattern to be found in server's stdout after certain line number
# return value is a list containing the line that matched the pattern and the line number
proc wait_for_log_messages {srv_idx patterns from_line maxtries delay} {
    set retry $maxtries
    set next_line [expr $from_line + 1] ;# searching form the line after
    set stdout [srv $srv_idx stdout]
    while {$retry} {
        # re-read the last line (unless it's before to our first), last time we read it, it might have been incomplete
        set next_line [expr $next_line - 1 > $from_line + 1 ? $next_line - 1 : $from_line + 1]
        set result [exec tail -n +$next_line < $stdout]
        set result [split $result "\n"]
        foreach line $result {
            foreach pattern $patterns {
                if {[string match $pattern $line]} {
                    return [list $line $next_line]
                }
            }
            incr next_line
        }
        incr retry -1
        after $delay
    }
    if {$retry == 0} {
        if {$::verbose} {
            puts "content of $stdout from line: $from_line:"
            puts [exec tail -n +$from_line < $stdout]
        }
        fail "log message of '$patterns' not found in $stdout after line: $from_line till line: [expr $next_line -1]"
    }
}

# write line to server log file
proc write_log_line {srv_idx msg} {
    set logfile [srv $srv_idx stdout]
    set fd [open $logfile "a+"]
    puts $fd "### $msg"
    close $fd
}

# Random integer between 0 and max (excluded).
proc randomInt {max} {
    expr {int(rand()*$max)}
}

# Random integer between min and max (excluded).
proc randomRange {min max} {
    expr {int(rand()*[expr $max - $min]) + $min}
}

# Random signed integer between -max and max (both extremes excluded).
proc randomSignedInt {max} {
    set i [randomInt $max]
    if {rand() > 0.5} {
        set i -$i
    }
    return $i
}

proc randpath args {
    set path [expr {int(rand()*[llength $args])}]
    uplevel 1 [lindex $args $path]
}

proc randomValue {} {
    randpath {
        # Small enough to likely collide
        randomSignedInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomSignedInt 2000000000} {randomSignedInt 4000000000}
    } {
        # 64 bit
        randpath {randomSignedInt 1000000000000}
    } {
        # Random string
        randpath {randstring 0 256 alpha} \
                {randstring 0 256 compr} \
                {randstring 0 256 binary}
    }
}

proc randomKey {} {
    randpath {
        # Small enough to likely collide
        randomInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomInt 2000000000} {randomInt 4000000000}
    } {
        # 64 bit
        randpath {randomInt 1000000000000}
    } {
        # Random string
        randpath {randstring 1 256 alpha} \
                {randstring 1 256 compr}
    }
}

proc findKeyWithType {r type} {
    for {set j 0} {$j < 20} {incr j} {
        set k [{*}$r randomkey]
        if {$k eq {}} {
            return {}
        }
        if {[{*}$r type $k] eq $type} {
            return $k
        }
    }
    return {}
}

proc createComplexDataset {r ops {opt {}}} {
    set useexpire [expr {[lsearch -exact $opt useexpire] != -1}]
    if {[lsearch -exact $opt usetag] != -1} {
        set tag "{t}"
    } else {
        set tag ""
    }
    for {set j 0} {$j < $ops} {incr j} {
        set k [randomKey]$tag
        set k2 [randomKey]$tag
        set f [randomValue]
        set v [randomValue]

        if {$useexpire} {
            if {rand() < 0.1} {
                {*}$r expire [randomKey] [randomInt 2]
            }
        }

        randpath {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            randpath {set d +inf} {set d -inf}
        }
        set t [{*}$r type $k]

        if {$t eq {none}} {
            randpath {
                {*}$r set $k $v
            } {
                {*}$r lpush $k $v
            } {
                {*}$r sadd $k $v
            } {
                {*}$r zadd $k $d $v
            } {
                {*}$r hset $k $f $v
            } {
                {*}$r del $k
            }
            set t [{*}$r type $k]
        }

        switch $t {
            {string} {
                # Nothing to do
            }
            {list} {
                randpath {{*}$r lpush $k $v} \
                        {{*}$r rpush $k $v} \
                        {{*}$r lrem $k 0 $v} \
                        {{*}$r rpop $k} \
                        {{*}$r lpop $k}
            }
            {set} {
                randpath {{*}$r sadd $k $v} \
                        {{*}$r srem $k $v} \
                        {
                            set otherset [findKeyWithType {*}$r set]
                            if {$otherset ne {}} {
                                randpath {
                                    {*}$r sunionstore $k2 $k $otherset
                                } {
                                    {*}$r sinterstore $k2 $k $otherset
                                } {
                                    {*}$r sdiffstore $k2 $k $otherset
                                }
                            }
                        }
            }
            {zset} {
                randpath {{*}$r zadd $k $d $v} \
                        {{*}$r zrem $k $v} \
                        {
                            set otherzset [findKeyWithType {*}$r zset]
                            if {$otherzset ne {}} {
                                randpath {
                                    {*}$r zunionstore $k2 2 $k $otherzset
                                } {
                                    {*}$r zinterstore $k2 2 $k $otherzset
                                }
                            }
                        }
            }
            {hash} {
                randpath {{*}$r hset $k $f $v} \
                        {{*}$r hdel $k $f}
            }
        }
    }
}

proc formatCommand {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

proc csvdump r {
    set o {}
    if {$::singledb} {
        set maxdb 1
    } else {
        set maxdb 16
    }
    for {set db 0} {$db < $maxdb} {incr db} {
        if {!$::singledb} {
            {*}$r select $db
        }
        foreach k [lsort [{*}$r keys *]] {
            set type [{*}$r type $k]
            append o [csvstring $db] , [csvstring $k] , [csvstring $type] ,
            switch $type {
                string {
                    append o [csvstring [{*}$r get $k]] "\n"
                }
                list {
                    foreach e [{*}$r lrange $k 0 -1] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                set {
                    foreach e [lsort [{*}$r smembers $k]] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                zset {
                    foreach e [{*}$r zrange $k 0 -1 withscores] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                hash {
                    set fields [{*}$r hgetall $k]
                    set newfields {}
                    foreach {k v} $fields {
                        lappend newfields [list $k $v]
                    }
                    set fields [lsort -index 0 $newfields]
                    foreach kv $fields {
                        append o [csvstring [lindex $kv 0]] ,
                        append o [csvstring [lindex $kv 1]] ,
                    }
                    append o "\n"
                }
            }
        }
    }
    if {!$::singledb} {
        {*}$r select 9
    }
    return $o
}

proc csvstring s {
    return "\"$s\""
}

proc roundFloat f {
    format "%.10g" $f
}

set ::last_port_attempted 0
proc find_available_port {start count} {
    set port [expr $::last_port_attempted + 1]
    for {set attempts 0} {$attempts < $count} {incr attempts} {
        if {$port < $start || $port >= $start+$count} {
            set port $start
        }
        set fd1 -1
        if {[catch {set fd1 [socket -server 127.0.0.1 $port]}] ||
            [catch {set fd2 [socket -server 127.0.0.1 [expr $port+10000]]}]} {
            if {$fd1 != -1} {
                close $fd1
            }
        } else {
            close $fd1
            close $fd2
            set ::last_port_attempted $port
            return $port
        }
        incr port
    }
    error "Can't find a non busy port in the $start-[expr {$start+$count-1}] range."
}

# Test if TERM looks like to support colors
proc color_term {} {
    expr {[info exists ::env(TERM)] && [string match *xterm* $::env(TERM)]}
}

proc colorstr {color str} {
    if {[color_term]} {
        set b 0
        if {[string range $color 0 4] eq {bold-}} {
            set b 1
            set color [string range $color 5 end]
        }
        switch $color {
            red {set colorcode {31}}
            green {set colorcode {32}}
            yellow {set colorcode {33}}
            blue {set colorcode {34}}
            magenta {set colorcode {35}}
            cyan {set colorcode {36}}
            white {set colorcode {37}}
            default {set colorcode {37}}
        }
        if {$colorcode ne {}} {
            return "\033\[$b;${colorcode};49m$str\033\[0m"
        }
    } else {
        return $str
    }
}

proc find_valgrind_errors {stderr on_termination} {
    set fd [open $stderr]
    set buf [read $fd]
    close $fd

    # Look for stack trace (" at 0x") and other errors (Invalid, Mismatched, etc).
    # Look for "Warnings", but not the "set address range perms". These don't indicate any real concern.
    # corrupt-dump unit, not sure why but it seems they don't indicate any real concern.
    if {[regexp -- { at 0x} $buf] ||
        [regexp -- {^(?=.*Warning)(?:(?!set address range perms).)*$} $buf] ||
        [regexp -- {Invalid} $buf] ||
        [regexp -- {Mismatched} $buf] ||
        [regexp -- {uninitialized} $buf] ||
        [regexp -- {has a fishy} $buf] ||
        [regexp -- {overlap} $buf]} {
        return $buf
    }

    # If the process didn't terminate yet, we can't look for the summary report
    if {!$on_termination} {
        return ""
    }

    # Look for the absence of a leak free summary (happens when the server isn't terminated properly).
    if {(![regexp -- {definitely lost: 0 bytes} $buf] &&
         ![regexp -- {no leaks are possible} $buf])} {
        return $buf
    }

    return ""
}

### start gen_write_load 
set ::tlsdir "tls"

# Continuously sends SET commands to the node. If key is omitted, a random key is
# used for every SET command. The value is always random.
proc gen_write_load {host port seconds tls {key ""}} {
    set start_time [clock seconds]
    set r [valkey $host $port 1 $tls]
    $r client setname LOAD_HANDLER
    $r select 9
    while 1 {
        if {$key == ""} {
            $r set [expr rand()] [expr rand()]
        } else {
            $r set $key [expr rand()]
        }
        if {[clock seconds]-$start_time > $seconds} {
            exit 0
        }
    }
}

### end gen_Write_load

# Execute a background process writing random data for the specified number
# of seconds to the specified the server instance.
proc start_write_load {host port seconds} {
    set tclsh [info nameofexecutable]
    gen_write_load.tcl $host $port $seconds $::tls ""
}

# Execute a background process writing only one key for the specified number
# of seconds to the specified Redis instance. This load handler is useful for
# tests which requires heavy replication stream but no memory load. 
proc start_one_key_write_load {host port seconds key} {
    set tclsh [info nameofexecutable]
    gen_write_load $host $port $seconds $::tls $key
}

# Stop a process generating write load executed with start_write_load.
proc stop_write_load {handle} {
    catch {exec /bin/kill -9 $handle}
}

proc wait_load_handlers_disconnected {{level 0}} {
    wait_for_condition 50 100 {
        ![string match {*name=LOAD_HANDLER*} [r $level client list]]
    } else {
        fail "load_handler(s) still connected after too long time."
    }
}

proc K { x y } { set x } 

# Shuffle a list with Fisher-Yates algorithm.
proc lshuffle {list} {
    set n [llength $list]
    while {$n>1} {
        set j [expr {int(rand()*$n)}]
        incr n -1
        if {$n==$j} continue
        set v [lindex $list $j]
        lset list $j [lindex $list $n]
        lset list $n $v
    }
    return $list
}

### start bg_complex_data 

# Tcl client library - used by the server test
# Copyright (c) 2009-2014 Redis Ltd.
# Released under the BSD license like Redis itself
#
# Example usage:
#
# set r [valkey 127.0.0.1 6379]
# $r lpush mylist foo
# $r lpush mylist bar
# $r lrange mylist 0 -1
# $r close
#
# Non blocking usage example:
#
# proc handlePong {r type reply} {
#     puts "PONG $type '$reply'"
#     if {$reply ne "PONG"} {
#         $r ping [list handlePong]
#     }
# }
#
# set r [valkey]
# $r blocking 0
# $r get fo [list handlePong]
#
# vwait forever

proc randstring {min max {type binary}} {
    set len [expr {$min+int(rand()*($max-$min+1))}]
    set output {}
    if {$type eq {binary}} {
        set minval 0
        set maxval 255
    } elseif {$type eq {alpha} || $type eq {simplealpha}} {
        set minval 48
        set maxval 122
    } elseif {$type eq {compr}} {
        set minval 48
        set maxval 52
    }
    while {$len} {
        set num [expr {$minval+int(rand()*($maxval-$minval+1))}]
        set rr [format "%c" $num]
        if {$type eq {simplealpha} && ![string is alnum $rr]} {continue}
        if {$type eq {alpha} && $num eq 92} {continue} ;# avoid putting '\' char in the string, it can mess up TCL processing
        append output $rr
        incr len -1
    }
    return $output
}

# Useful for some test
proc zlistAlikeSort {a b} {
    if {[lindex $a 0] > [lindex $b 0]} {return 1}
    if {[lindex $a 0] < [lindex $b 0]} {return -1}
    string compare [lindex $a 1] [lindex $b 1]
}

# Return all log lines starting with the first line that contains a warning.
# Generally, this will be an assertion error with a stack trace.
proc crashlog_from_file {filename} {
    set lines [split [exec cat $filename] "\n"]
    set matched 0
    set logall 0
    set result {}
    foreach line $lines {
        if {[string match {*BUG REPORT START*} $line]} {
            set logall 1
        }
        if {[regexp {^\[\d+\]\s+\d+\s+\w+\s+\d{2}:\d{2}:\d{2} \#} $line]} {
            set matched 1
        }
        if {$logall || $matched} {
            lappend result $line
        }
    }
    join $result "\n"
}

# Return sanitizer log lines
proc sanitizer_errors_from_file {filename} {
    set log [exec cat $filename]
    set lines [split [exec cat $filename] "\n"]

    foreach line $lines {
        # Ignore huge allocation warnings
        if ([string match {*WARNING: AddressSanitizer failed to allocate*} $line]) {
            continue
        }

        # GCC UBSAN output does not contain 'Sanitizer' but 'runtime error'.
        if {[string match {*runtime error*} $line] ||
            [string match {*Sanitizer*} $line]} {
            return $log
        }
    }

    return ""
}

proc getInfoProperty {infostr property} {
    if {[regexp -lineanchor "^$property:(.*?)\r\n" $infostr _ value]} {
        return $value
    }
}

# Return value for INFO property
proc status {r property} {
    set _ [getInfoProperty [{*}$r info] $property]
}

proc waitForBgsave r {
    while 1 {
        if {[status $r rdb_bgsave_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background save to finish... "
                flush stdout
            }
            after 50
        } else {
            break
        }
    }
}

proc waitForBgrewriteaof r {
    while 1 {
        if {[status $r aof_rewrite_in_progress] eq 1} {
            if {$::verbose} {
                puts -nonewline "\nWaiting for background AOF rewrite to finish... "
                flush stdout
            }
            after 50
        } else {
            break
        }
    }
}

proc wait_for_sync r {
    wait_for_condition 50 100 {
        [status $r master_link_status] eq "up"
    } else {
        fail "replica didn't sync in time"
    }
}

proc wait_replica_online r {
    wait_for_condition 50 100 {
        [string match "*slave0:*,state=online*" [$r info replication]]
    } else {
        fail "replica didn't online in time"
    }
}

proc wait_for_ofs_sync {r1 r2} {
    wait_for_condition 50 100 {
        [status $r1 master_repl_offset] eq [status $r2 master_repl_offset]
    } else {
        fail "replica offset didn't match in time"
    }
}

proc wait_done_loading r {
    wait_for_condition 50 100 {
        [catch {$r ping} e] == 0
    } else {
        fail "Loading DB is taking too much time."
    }
}

proc wait_lazyfree_done r {
    wait_for_condition 50 100 {
        [status $r lazyfree_pending_objects] == 0
    } else {
        fail "lazyfree isn't done"
    }
}

# count current log lines in server's stdout
proc count_log_lines {srv_idx} {
    set _ [string trim [exec wc -l < [srv $srv_idx stdout]]]
}

# returns the number of times a line with that pattern appears in a file
proc count_message_lines {file pattern} {
    set res 0
    # exec fails when grep exists with status other than 0 (when the pattern wasn't found)
    catch {
        set res [string trim [exec grep $pattern $file 2> /dev/null | wc -l]]
    }
    return $res
}

# returns the number of times a line with that pattern appears in the log
proc count_log_message {srv_idx pattern} {
    set stdout [srv $srv_idx stdout]
    return [count_message_lines $stdout $pattern]
}

# verify pattern exists in server's stdout after a certain line number
proc verify_log_message {srv_idx pattern from_line} {
    incr from_line
    set result [exec tail -n +$from_line < [srv $srv_idx stdout]]
    if {![string match $pattern $result]} {
        fail "expected message not found in log file: $pattern"
    }
}

# verify pattern does not exists in server's stout after a certain line number
proc verify_no_log_message {srv_idx pattern from_line} {
    incr from_line
    set result [exec tail -n +$from_line < [srv $srv_idx stdout]]
    if {[string match $pattern $result]} {
        fail "expected message found in log file: $pattern"
    }
}

# wait for pattern to be found in server's stdout after certain line number
# return value is a list containing the line that matched the pattern and the line number
proc wait_for_log_messages {srv_idx patterns from_line maxtries delay} {
    set retry $maxtries
    set next_line [expr $from_line + 1] ;# searching form the line after
    set stdout [srv $srv_idx stdout]
    while {$retry} {
        # re-read the last line (unless it's before to our first), last time we read it, it might have been incomplete
        set next_line [expr $next_line - 1 > $from_line + 1 ? $next_line - 1 : $from_line + 1]
        set result [exec tail -n +$next_line < $stdout]
        set result [split $result "\n"]
        foreach line $result {
            foreach pattern $patterns {
                if {[string match $pattern $line]} {
                    return [list $line $next_line]
                }
            }
            incr next_line
        }
        incr retry -1
        after $delay
    }
    if {$retry == 0} {
        if {$::verbose} {
            puts "content of $stdout from line: $from_line:"
            puts [exec tail -n +$from_line < $stdout]
        }
        fail "log message of '$patterns' not found in $stdout after line: $from_line till line: [expr $next_line -1]"
    }
}

# write line to server log file
proc write_log_line {srv_idx msg} {
    set logfile [srv $srv_idx stdout]
    set fd [open $logfile "a+"]
    puts $fd "### $msg"
    close $fd
}

# Random integer between 0 and max (excluded).
proc randomInt {max} {
    expr {int(rand()*$max)}
}

# Random integer between min and max (excluded).
proc randomRange {min max} {
    expr {int(rand()*[expr $max - $min]) + $min}
}

# Random signed integer between -max and max (both extremes excluded).
proc randomSignedInt {max} {
    set i [randomInt $max]
    if {rand() > 0.5} {
        set i -$i
    }
    return $i
}

proc randpath args {
    set path [expr {int(rand()*[llength $args])}]
    uplevel 1 [lindex $args $path]
}

proc randomValue {} {
    randpath {
        # Small enough to likely collide
        randomSignedInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomSignedInt 2000000000} {randomSignedInt 4000000000}
    } {
        # 64 bit
        randpath {randomSignedInt 1000000000000}
    } {
        # Random string
        randpath {randstring 0 256 alpha} \
                {randstring 0 256 compr} \
                {randstring 0 256 binary}
    }
}

proc randomKey {} {
    randpath {
        # Small enough to likely collide
        randomInt 1000
    } {
        # 32 bit compressible signed/unsigned
        randpath {randomInt 2000000000} {randomInt 4000000000}
    } {
        # 64 bit
        randpath {randomInt 1000000000000}
    } {
        # Random string
        randpath {randstring 1 256 alpha} \
                {randstring 1 256 compr}
    }
}

proc findKeyWithType {r type} {
    for {set j 0} {$j < 20} {incr j} {
        set k [{*}$r randomkey]
        if {$k eq {}} {
            return {}
        }
        if {[{*}$r type $k] eq $type} {
            return $k
        }
    }
    return {}
}

proc createComplexDataset {r ops {opt {}}} {
    set useexpire [expr {[lsearch -exact $opt useexpire] != -1}]
    if {[lsearch -exact $opt usetag] != -1} {
        set tag "{t}"
    } else {
        set tag ""
    }
    for {set j 0} {$j < $ops} {incr j} {
        set k [randomKey]$tag
        set k2 [randomKey]$tag
        set f [randomValue]
        set v [randomValue]

        if {$useexpire} {
            if {rand() < 0.1} {
                {*}$r expire [randomKey] [randomInt 2]
            }
        }

        randpath {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            set d [expr {rand()}]
        } {
            randpath {set d +inf} {set d -inf}
        }
        set t [{*}$r type $k]

        if {$t eq {none}} {
            randpath {
                {*}$r set $k $v
            } {
                {*}$r lpush $k $v
            } {
                {*}$r sadd $k $v
            } {
                {*}$r zadd $k $d $v
            } {
                {*}$r hset $k $f $v
            } {
                {*}$r del $k
            }
            set t [{*}$r type $k]
        }

        switch $t {
            {string} {
                # Nothing to do
            }
            {list} {
                randpath {{*}$r lpush $k $v} \
                        {{*}$r rpush $k $v} \
                        {{*}$r lrem $k 0 $v} \
                        {{*}$r rpop $k} \
                        {{*}$r lpop $k}
            }
            {set} {
                randpath {{*}$r sadd $k $v} \
                        {{*}$r srem $k $v} \
                        {
                            set otherset [findKeyWithType {*}$r set]
                            if {$otherset ne {}} {
                                randpath {
                                    {*}$r sunionstore $k2 $k $otherset
                                } {
                                    {*}$r sinterstore $k2 $k $otherset
                                } {
                                    {*}$r sdiffstore $k2 $k $otherset
                                }
                            }
                        }
            }
            {zset} {
                randpath {{*}$r zadd $k $d $v} \
                        {{*}$r zrem $k $v} \
                        {
                            set otherzset [findKeyWithType {*}$r zset]
                            if {$otherzset ne {}} {
                                randpath {
                                    {*}$r zunionstore $k2 2 $k $otherzset
                                } {
                                    {*}$r zinterstore $k2 2 $k $otherzset
                                }
                            }
                        }
            }
            {hash} {
                randpath {{*}$r hset $k $f $v} \
                        {{*}$r hdel $k $f}
            }
        }
    }
}

proc formatCommand {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

proc csvdump r {
    set o {}
    if {$::singledb} {
        set maxdb 1
    } else {
        set maxdb 16
    }
    for {set db 0} {$db < $maxdb} {incr db} {
        if {!$::singledb} {
            {*}$r select $db
        }
        foreach k [lsort [{*}$r keys *]] {
            set type [{*}$r type $k]
            append o [csvstring $db] , [csvstring $k] , [csvstring $type] ,
            switch $type {
                string {
                    append o [csvstring [{*}$r get $k]] "\n"
                }
                list {
                    foreach e [{*}$r lrange $k 0 -1] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                set {
                    foreach e [lsort [{*}$r smembers $k]] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                zset {
                    foreach e [{*}$r zrange $k 0 -1 withscores] {
                        append o [csvstring $e] ,
                    }
                    append o "\n"
                }
                hash {
                    set fields [{*}$r hgetall $k]
                    set newfields {}
                    foreach {k v} $fields {
                        lappend newfields [list $k $v]
                    }
                    set fields [lsort -index 0 $newfields]
                    foreach kv $fields {
                        append o [csvstring [lindex $kv 0]] ,
                        append o [csvstring [lindex $kv 1]] ,
                    }
                    append o "\n"
                }
            }
        }
    }
    if {!$::singledb} {
        {*}$r select 9
    }
    return $o
}

proc csvstring s {
    return "\"$s\""
}

proc roundFloat f {
    format "%.10g" $f
}

set ::last_port_attempted 0
proc find_available_port {start count} {
    set port [expr $::last_port_attempted + 1]
    for {set attempts 0} {$attempts < $count} {incr attempts} {
        if {$port < $start || $port >= $start+$count} {
            set port $start
        }
        set fd1 -1
        if {[catch {set fd1 [socket -server 127.0.0.1 $port]}] ||
            [catch {set fd2 [socket -server 127.0.0.1 [expr $port+10000]]}]} {
            if {$fd1 != -1} {
                close $fd1
            }
        } else {
            close $fd1
            close $fd2
            set ::last_port_attempted $port
            return $port
        }
        incr port
    }
    error "Can't find a non busy port in the $start-[expr {$start+$count-1}] range."
}

# Test if TERM looks like to support colors
proc color_term {} {
    expr {[info exists ::env(TERM)] && [string match *xterm* $::env(TERM)]}
}

proc colorstr {color str} {
    if {[color_term]} {
        set b 0
        if {[string range $color 0 4] eq {bold-}} {
            set b 1
            set color [string range $color 5 end]
        }
        switch $color {
            red {set colorcode {31}}
            green {set colorcode {32}}
            yellow {set colorcode {33}}
            blue {set colorcode {34}}
            magenta {set colorcode {35}}
            cyan {set colorcode {36}}
            white {set colorcode {37}}
            default {set colorcode {37}}
        }
        if {$colorcode ne {}} {
            return "\033\[$b;${colorcode};49m$str\033\[0m"
        }
    } else {
        return $str
    }
}

proc find_valgrind_errors {stderr on_termination} {
    set fd [open $stderr]
    set buf [read $fd]
    close $fd

    # Look for stack trace (" at 0x") and other errors (Invalid, Mismatched, etc).
    # Look for "Warnings", but not the "set address range perms". These don't indicate any real concern.
    # corrupt-dump unit, not sure why but it seems they don't indicate any real concern.
    if {[regexp -- { at 0x} $buf] ||
        [regexp -- {^(?=.*Warning)(?:(?!set address range perms).)*$} $buf] ||
        [regexp -- {Invalid} $buf] ||
        [regexp -- {Mismatched} $buf] ||
        [regexp -- {uninitialized} $buf] ||
        [regexp -- {has a fishy} $buf] ||
        [regexp -- {overlap} $buf]} {
        return $buf
    }

    # If the process didn't terminate yet, we can't look for the summary report
    if {!$on_termination} {
        return ""
    }

    # Look for the absence of a leak free summary (happens when the server isn't terminated properly).
    if {(![regexp -- {definitely lost: 0 bytes} $buf] &&
         ![regexp -- {no leaks are possible} $buf])} {
        return $buf
    }

    return ""
}

# Execute a background process writing random data for the specified number
# of seconds to the specified the server instance.
proc start_write_load {host port seconds} {
    set tclsh [info nameofexecutable]
    exec $tclsh gen_write_load.tcl $host $port $seconds $::tls "" &
}

# Execute a background process writing only one key for the specified number
# of seconds to the specified Redis instance. This load handler is useful for
# tests which requires heavy replication stream but no memory load. 
proc start_one_key_write_load {host port seconds key} {
    set tclsh [info nameofexecutable]
    exec $tclsh gen_write_load.tcl $host $port $seconds $::tls $key &
}

# Stop a process generating write load executed with start_write_load.
proc stop_write_load {handle} {
    catch {exec /bin/kill -9 $handle}
}

proc wait_load_handlers_disconnected {{level 0}} {
    wait_for_condition 50 100 {
        ![string match {*name=LOAD_HANDLER*} [r $level client list]]
    } else {
        fail "load_handler(s) still connected after too long time."
    }
}

proc K { x y } { set x } 

# Shuffle a list with Fisher-Yates algorithm.
proc lshuffle {list} {
    set n [llength $list]
    while {$n>1} {
        set j [expr {int(rand()*$n)}]
        incr n -1
        if {$n==$j} continue
        set v [lindex $list $j]
        lset list $j [lindex $list $n]
        lset list $n $v
    }
    return $list
}

# Execute a background process writing complex data for the specified number
# of ops to the specified server instance.
proc start_bg_complex_data {host port db ops} {
    set tclsh [info nameofexecutable]
    exec $tclsh bg_complex_data.tcl $host $port $db $ops $::tls &
}

# Stop a process generating write load executed with start_bg_complex_data.
proc stop_bg_complex_data {handle} {
    catch {exec /bin/kill -9 $handle}
}

# Write num keys with the given key prefix and value size (in bytes). If idx is
# given, it's the index (AKA level) used with the srv procedure and it specifies
# to which server instance to write the keys.
proc populate {num {prefix key:} {size 3} {idx 0} {prints false} {expires 0}} {
    r $idx deferred 1
    if {$num > 16} {set pipeline 16} else {set pipeline $num}
    set val [string repeat A $size]
    for {set j 0} {$j < $pipeline} {incr j} {
        if {$expires > 0} {
            r $idx set $prefix$j $val ex $expires
        } else {
            r $idx set $prefix$j $val
        }
        if {$prints} {puts $j}
    }
    for {} {$j < $num} {incr j} {
        if {$expires > 0} {
            r $idx set $prefix$j $val ex $expires
        } else {
            r $idx set $prefix$j $val
        }
        r $idx read
        if {$prints} {puts $j}
    }
    for {set j 0} {$j < $pipeline} {incr j} {
        r $idx read
        if {$prints} {puts $j}
    }
    r $idx deferred 0
}

proc get_child_pid {idx} {
    set pid [srv $idx pid]
    if {[file exists "/usr/bin/pgrep"]} {
        set fd [open "|pgrep -P $pid" "r"]
        set child_pid [string trim [lindex [split [read $fd] \n] 0]]
    } else {
        set fd [open "|ps --ppid $pid -o pid" "r"]
        set child_pid [string trim [lindex [split [read $fd] \n] 1]]
    }
    close $fd

    return $child_pid
}

proc process_is_alive pid {
    if {[catch {exec ps -p $pid -f} err]} {
        return 0
    } else {
        if {[string match "*<defunct>*" $err]} { return 0 }
        return 1
    }
}

# Return true if the specified process is paused by pause_process.
proc process_is_paused pid {
    return [string match {*T*} [lindex [exec ps j $pid] 16]]
}

proc pause_process pid {
    exec kill -SIGSTOP $pid
    wait_for_condition 50 100 {
        [string match {*T*} [lindex [exec ps j $pid] 16]]
    } else {
        puts [exec ps j $pid]
        fail "process didn't stop"
    }
}

proc resume_process pid {
    exec kill -SIGCONT $pid
}

proc cmdrstat {cmd r} {
    if {[regexp "\r\ncmdstat_$cmd:(.*?)\r\n" [$r info commandstats] _ value]} {
        set _ $value
    }
}

proc errorrstat {cmd r} {
    if {[regexp "\r\nerrorstat_$cmd:(.*?)\r\n" [$r info errorstats] _ value]} {
        set _ $value
    }
}

proc latencyrstat_percentiles {cmd r} {
    if {[regexp "\r\nlatency_percentiles_usec_$cmd:(.*?)\r\n" [$r info latencystats] _ value]} {
        set _ $value
    }
}

proc generate_fuzzy_traffic_on_key {key duration} {
    # Commands per type, blocking commands removed
    # TODO: extract these from COMMAND DOCS, and improve to include other types
    set string_commands {APPEND BITCOUNT BITFIELD BITOP BITPOS DECR DECRBY GET GETBIT GETRANGE GETSET INCR INCRBY INCRBYFLOAT MGET MSET MSETNX PSETEX SET SETBIT SETEX SETNX SETRANGE LCS STRLEN}
    set hash_commands {HDEL HEXISTS HGET HGETALL HINCRBY HINCRBYFLOAT HKEYS HLEN HMGET HMSET HSCAN HSET HSETNX HSTRLEN HVALS HRANDFIELD}
    set zset_commands {ZADD ZCARD ZCOUNT ZINCRBY ZINTERSTORE ZLEXCOUNT ZPOPMAX ZPOPMIN ZRANGE ZRANGEBYLEX ZRANGEBYSCORE ZRANK ZREM ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANGE ZREVRANGEBYLEX ZREVRANGEBYSCORE ZREVRANK ZSCAN ZSCORE ZUNIONSTORE ZRANDMEMBER}
    set list_commands {LINDEX LINSERT LLEN LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM RPOP RPOPLPUSH RPUSH RPUSHX}
    set set_commands {SADD SCARD SDIFF SDIFFSTORE SINTER SINTERSTORE SISMEMBER SMEMBERS SMOVE SPOP SRANDMEMBER SREM SSCAN SUNION SUNIONSTORE}
    set stream_commands {XACK XADD XCLAIM XDEL XGROUP XINFO XLEN XPENDING XRANGE XREAD XREADGROUP XREVRANGE XTRIM}
    set commands [dict create string $string_commands hash $hash_commands zset $zset_commands list $list_commands set $set_commands stream $stream_commands]

    set type [r type $key]
    set cmds [dict get $commands $type]
    set start_time [clock seconds]
    set sent {}
    set succeeded 0
    while {([clock seconds]-$start_time) < $duration} {
        # find a random command for our key type
        set cmd_idx [expr {int(rand()*[llength $cmds])}]
        set cmd [lindex $cmds $cmd_idx]
        # get the command details from the server
        if { [ catch {
            set cmd_info [lindex [r command info $cmd] 0]
        } err ] } {
            # if we failed, it means the server crashed after the previous command
            return $sent
        }
        # try to build a valid command argument
        set arity [lindex $cmd_info 1]
        set arity [expr $arity < 0 ? - $arity: $arity]
        set firstkey [lindex $cmd_info 3]
        set lastkey [lindex $cmd_info 4]
        set i 1
        if {$cmd == "XINFO"} {
            lappend cmd "STREAM"
            lappend cmd $key
            lappend cmd "FULL"
            incr i 3
        }
        if {$cmd == "XREAD"} {
            lappend cmd "STREAMS"
            lappend cmd $key
            randpath {
                lappend cmd \$
            } {
                lappend cmd [randomValue]
            }
            incr i 3
        }
        if {$cmd == "XADD"} {
            lappend cmd $key
            randpath {
                lappend cmd "*"
            } {
                lappend cmd [randomValue]
            }
            lappend cmd [randomValue]
            lappend cmd [randomValue]
            incr i 4
        }
        for {} {$i < $arity} {incr i} {
            if {$i == $firstkey || $i == $lastkey} {
                lappend cmd $key
            } else {
                lappend cmd [randomValue]
            }
        }
        # execute the command, we expect commands to fail on syntax errors
        lappend sent $cmd
        if { ! [ catch {
            r {*}$cmd
        } err ] } {
            incr succeeded
        } else {
            set err [format "%s" $err] ;# convert to string for pattern matching
            if {[string match "*SIGTERM*" $err]} {
                puts "commands caused test to hang:"
                foreach cmd $sent {
                    foreach arg $cmd {
                        puts -nonewline "[string2printable $arg] "
                    }
                    puts ""
                }
                # Re-raise, let handler up the stack take care of this.
                error $err $::errorInfo
            }
        }
    }

    # print stats so that we know if we managed to generate commands that actually made sense
    #if {$::verbose} {
    #    set count [llength $sent]
    #    puts "Fuzzy traffic sent: $count, succeeded: $succeeded"
    #}

    # return the list of commands we sent
    return $sent
}

proc string2printable s {
    set res {}
    set has_special_chars false
    foreach i [split $s {}] {
        scan $i %c int
        # non printable characters, including space and excluding: " \ $ { }
        if {$int < 32 || $int > 122 || $int == 34 || $int == 36 || $int == 92} {
            set has_special_chars true
        }
        # TCL8.5 has issues mixing \x notation and normal chars in the same
        # source code string, so we'll convert the entire string.
        append res \\x[format %02X $int]
    }
    if {!$has_special_chars} {
        return $s
    }
    set res "\"$res\""
    return $res
}

# Calculation value of Chi-Square Distribution. By this value
# we can verify the random distribution sample confidence.
# Based on the following wiki:
# https://en.wikipedia.org/wiki/Chi-square_distribution
#
# param res    Random sample list
# return       Value of Chi-Square Distribution
#
# x2_value: return of chi_square_value function
# df: Degrees of freedom, Number of independent values minus 1
#
# By using x2_value and df to back check the cardinality table,
# we can know the confidence of the random sample.
proc chi_square_value {res} {
    unset -nocomplain mydict
    foreach key $res {
        dict incr mydict $key 1
    }

    set x2_value 0
    set p [expr [llength $res] / [dict size $mydict]]
    foreach key [dict keys $mydict] {
        set value [dict get $mydict $key]

        # Aggregate the chi-square value of each element
        set v [expr {pow($value - $p, 2) / $p}]
        set x2_value [expr {$x2_value + $v}]
    }

    return $x2_value
}

#subscribe to Pub/Sub channels
proc consume_subscribe_messages {client type channels} {
    set numsub -1
    set counts {}

    for {set i [llength $channels]} {$i > 0} {incr i -1} {
        set msg [$client read]
        assert_equal $type [lindex $msg 0]

        # when receiving subscribe messages the channels names
        # are ordered. when receiving unsubscribe messages
        # they are unordered
        set idx [lsearch -exact $channels [lindex $msg 1]]
        if {[string match "*unsubscribe" $type]} {
            assert {$idx >= 0}
        } else {
            assert {$idx == 0}
        }
        set channels [lreplace $channels $idx $idx]

        # aggregate the subscription count to return to the caller
        lappend counts [lindex $msg 2]
    }

    # we should have received messages for channels
    assert {[llength $channels] == 0}
    return $counts
}

proc subscribe {client channels} {
    $client subscribe {*}$channels
    consume_subscribe_messages $client subscribe $channels
}

proc ssubscribe {client channels} {
    $client ssubscribe {*}$channels
    consume_subscribe_messages $client ssubscribe $channels
}

proc unsubscribe {client {channels {}}} {
    $client unsubscribe {*}$channels
    consume_subscribe_messages $client unsubscribe $channels
}

proc sunsubscribe {client {channels {}}} {
    $client sunsubscribe {*}$channels
    consume_subscribe_messages $client sunsubscribe $channels
}

proc psubscribe {client channels} {
    $client psubscribe {*}$channels
    consume_subscribe_messages $client psubscribe $channels
}

proc punsubscribe {client {channels {}}} {
    $client punsubscribe {*}$channels
    consume_subscribe_messages $client punsubscribe $channels
}

proc debug_digest_value {key} {
    if {[lsearch $::denytags "needs:debug"] >= 0 || $::ignoredigest} {
        return "dummy-digest-value"
    }
    r debug digest-value $key
}

proc debug_digest {{level 0}} {
    if {[lsearch $::denytags "needs:debug"] >= 0 || $::ignoredigest} {
        return "dummy-digest"
    }
    r $level debug digest
}

proc wait_for_blocked_client {{idx 0}} {
    wait_for_condition 50 100 {
        [s $idx blocked_clients] ne 0
    } else {
        fail "no blocked clients"
    }
}

proc wait_for_blocked_clients_count {count {maxtries 100} {delay 10} {idx 0}} {
    wait_for_condition $maxtries $delay  {
        [s $idx blocked_clients] == $count
    } else {
        fail "Timeout waiting for blocked clients"
    }
}

proc wait_for_watched_clients_count {count {maxtries 100} {delay 10} {idx 0}} {
    wait_for_condition $maxtries $delay  {
        [s $idx watching_clients] == $count
    } else {
        fail "Timeout waiting for watched clients"
    }
}

proc read_from_aof {fp} {
    # Input fp is a blocking binary file descriptor of an opened AOF file.
    if {[gets $fp count] == -1} return ""
    set count [string range $count 1 end]

    # Return a list of arguments for the command.
    set res {}
    for {set j 0} {$j < $count} {incr j} {
        read $fp 1
        set arg [::valkey::valkey_bulk_read $fp]
        if {$j == 0} {set arg [string tolower $arg]}
        lappend res $arg
    }
    return $res
}

proc assert_aof_content {aof_path patterns} {
    set fp [open $aof_path r]
    fconfigure $fp -translation binary
    fconfigure $fp -blocking 1

    for {set j 0} {$j < [llength $patterns]} {incr j} {
        assert_match [lindex $patterns $j] [read_from_aof $fp]
    }
}

proc config_set {param value {options {}}} {
    set mayfail 0
    foreach option $options {
        switch $option {
            "mayfail" {
                set mayfail 1
            }
            default {
                error "Unknown option $option"
            }
        }
    }

    if {[catch {r config set $param $value} err]} {
        if {!$mayfail} {
            error $err
        } else {
            if {$::verbose} {
                puts "Ignoring CONFIG SET $param $value failure: $err"
            }
        }
    }
}

proc config_get_set {param value {options {}}} {
    set config [lindex [r config get $param] 1]
    config_set $param $value $options
    return $config
}

proc delete_lines_with_pattern {filename tmpfilename pattern} {
    set fh_in [open $filename r]
    set fh_out [open $tmpfilename w]
    while {[gets $fh_in line] != -1} {
        if {![regexp $pattern $line]} {
            puts $fh_out $line
        }
    }
    close $fh_in
    close $fh_out
    file rename -force $tmpfilename $filename
}

proc get_nonloopback_addr {} {
    set addrlist [list {}]
    catch { set addrlist [exec hostname -I] }
    return [lindex $addrlist 0]
}

proc get_nonloopback_client {} {
    return [valkey [get_nonloopback_addr] [srv 0 "port"] 0 $::tls]
}

# The following functions and variables are used only when running large-memory
# tests. We avoid defining them when not running large-memory tests because the 
# global variables takes up lots of memory.
proc init_large_mem_vars {} {
    if {![info exists ::str500]} {
        set ::str500 [string repeat x 500000000] ;# 500mb
        set ::str500_len [string length $::str500]
    }
}

# Utility function to write big argument into a server client connection
proc write_big_bulk {size {prefix ""} {skip_read no}} {
    init_large_mem_vars

    assert {[string length prefix] <= $size}
    r write "\$$size\r\n"
    r write $prefix
    incr size -[string length $prefix]
    while {$size >= 500000000} {
        r write $::str500
        incr size -500000000
    }
    if {$size > 0} {
        r write [string repeat x $size]
    }
    r write "\r\n"
    if {!$skip_read} {
        r flush
        r read
    }
}

# Utility to read big bulk response (work around Tcl limitations)
proc read_big_bulk {code {compare no} {prefix ""}} {
    init_large_mem_vars

    r readraw 1
    set resp_len [uplevel 1 $code] ;# get the first line of the RESP response
    assert_equal [string range $resp_len 0 0] "$"
    set resp_len [string range $resp_len 1 end]
    set prefix_len [string length $prefix]
    if {$compare} {
        assert {$prefix_len <= $resp_len}
        assert {$prefix_len <= $::str500_len}
    }

    set remaining $resp_len
    while {$remaining > 0} {
        set l $remaining
        if {$l > $::str500_len} {set l $::str500_len} ; # can't read more than 2gb at a time, so read 500mb so we can easily verify read data
        set read_data [r rawread $l]
        set nbytes [string length $read_data]
        if {$compare} {
            set comp_len $nbytes
            # Compare prefix part
            if {$remaining == $resp_len} {
                assert_equal $prefix [string range $read_data 0 [expr $prefix_len - 1]]
                set read_data [string range $read_data $prefix_len $nbytes]
                incr comp_len -$prefix_len
            }
            # Compare rest of data, evaluate and then assert to avoid huge print in case of failure
            set data_equal [expr {$read_data == [string range $::str500 0 [expr $comp_len - 1]]}]
            assert $data_equal
        }
        incr remaining -$nbytes
    }
    assert_equal [r rawread 2] "\r\n"
    r readraw 0
    return $resp_len
}

proc prepare_value {size} {
    set _v "c"
    for {set i 1} {$i < $size} {incr i} {
        append _v 0
    }
    return $_v
}

proc memory_usage {key} {
    set usage [r memory usage $key]
    if {![string match {*jemalloc*} [s mem_allocator]]} {
        # libc allocator can sometimes return a different size allocation for the same requested size
        # this makes tests that rely on MEMORY USAGE unreliable, so instead we return a constant 1
        set usage 1
    }
    return $usage
}

# forward compatibility, lmap missing in TCL 8.5
proc lmap args {
    set body [lindex $args end]
    set args [lrange $args 0 end-1]
    set n 0
    set pairs [list]
    foreach {varnames listval} $args {
        set varlist [list]
        foreach varname $varnames {
            upvar 1 $varname var$n
            lappend varlist var$n
            incr n
        }
        lappend pairs $varlist $listval
    }
    set temp [list]
    foreach {*}$pairs {
        lappend temp [uplevel 1 $body]
    }
    set temp
}

proc format_command {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

# Returns whether or not the system supports stack traces
proc system_backtrace_supported {} {
    set system_name [string tolower [exec uname -s]]
    if {$system_name eq {darwin}} {
        return 1
    } elseif {$system_name ne {linux}} {
        return 0
    }

    # libmusl does not support backtrace. Also return 0 on
    # static binaries (ldd exit code 1) where we can't detect libmusl
    catch {
        set ldd [exec ldd src/valkey-server]
        if {![string match {*libc.*musl*} $ldd]} {
            return 1
        }
    }
    return 0
}

proc generate_largevalue_test_array {} {
    array set largevalue {}
    set largevalue(listpack) "hello"
    set largevalue(quicklist) [string repeat "x" 8192]
    return [array get largevalue]
}

# Breakpoint function, which invokes a minimal debugger.
# This function can be placed within the desired Tcl tests for debugging purposes.
# 
# Arguments:
# * 's': breakpoint label, which is printed when breakpoints are hit for unique identification.
# 
# Source: https://wiki.tcl-lang.org/page/A+minimal+debugger
proc bp {{s {}}} {
    if ![info exists ::bp_skip] {
        set ::bp_skip [list]
    } elseif {[lsearch -exact $::bp_skip $s]>=0} return
    if [catch {info level -1} who] {set who ::}
    while 1 {
        puts -nonewline "$who/$s> "; flush stdout
        gets stdin line
        if {$line=="c"} {puts "continuing.."; break}
        if {$line=="i"} {set line "info locals"}
        catch {uplevel 1 $line} res
        puts $res
    }
}

proc bg_complex_data {host port db ops tls} {
    set r [valkey $host $port 0 $tls]
    $r client setname LOAD_HANDLER
    $r select $db
    createComplexDataset $r $ops
}

### end bg_complex_data

# Execute a background process writing complex data for the specified number
# of ops to the specified server instance.
proc start_bg_complex_data {host port db ops} {
    set tclsh [info nameofexecutable]
    bg_complex_data.tcl $host $port $db $ops $::tls 
}

# Stop a process generating write load executed with start_bg_complex_data.
proc stop_bg_complex_data {handle} {
    catch {exec /bin/kill -9 $handle}
}

# Write num keys with the given key prefix and value size (in bytes). If idx is
# given, it's the index (AKA level) used with the srv procedure and it specifies
# to which server instance to write the keys.
proc populate {num {prefix key:} {size 3} {idx 0} {prints false} {expires 0}} {
    r $idx deferred 1
    if {$num > 16} {set pipeline 16} else {set pipeline $num}
    set val [string repeat A $size]
    for {set j 0} {$j < $pipeline} {incr j} {
        if {$expires > 0} {
            r $idx set $prefix$j $val ex $expires
        } else {
            r $idx set $prefix$j $val
        }
        if {$prints} {puts $j}
    }
    for {} {$j < $num} {incr j} {
        if {$expires > 0} {
            r $idx set $prefix$j $val ex $expires
        } else {
            r $idx set $prefix$j $val
        }
        r $idx read
        if {$prints} {puts $j}
    }
    for {set j 0} {$j < $pipeline} {incr j} {
        r $idx read
        if {$prints} {puts $j}
    }
    r $idx deferred 0
}

proc get_child_pid {idx} {
    set pid [srv $idx pid]
    if {[file exists "/usr/bin/pgrep"]} {
        set fd [open "|pgrep -P $pid" "r"]
        set child_pid [string trim [lindex [split [read $fd] \n] 0]]
    } else {
        set fd [open "|ps --ppid $pid -o pid" "r"]
        set child_pid [string trim [lindex [split [read $fd] \n] 1]]
    }
    close $fd

    return $child_pid
}

proc process_is_alive pid {
    if {[catch {exec ps -p $pid -f} err]} {
        return 0
    } else {
        if {[string match "*<defunct>*" $err]} { return 0 }
        return 1
    }
}

# Return true if the specified process is paused by pause_process.
proc process_is_paused pid {
    return [string match {*T*} [lindex [exec ps j $pid] 16]]
}

proc pause_process pid {
    exec kill -SIGSTOP $pid
    wait_for_condition 50 100 {
        [string match {*T*} [lindex [exec ps j $pid] 16]]
    } else {
        puts [exec ps j $pid]
        fail "process didn't stop"
    }
}

proc resume_process pid {
    exec kill -SIGCONT $pid
}

proc cmdrstat {cmd r} {
    if {[regexp "\r\ncmdstat_$cmd:(.*?)\r\n" [$r info commandstats] _ value]} {
        set _ $value
    }
}

proc errorrstat {cmd r} {
    if {[regexp "\r\nerrorstat_$cmd:(.*?)\r\n" [$r info errorstats] _ value]} {
        set _ $value
    }
}

proc latencyrstat_percentiles {cmd r} {
    if {[regexp "\r\nlatency_percentiles_usec_$cmd:(.*?)\r\n" [$r info latencystats] _ value]} {
        set _ $value
    }
}

proc generate_fuzzy_traffic_on_key {key duration} {
    # Commands per type, blocking commands removed
    # TODO: extract these from COMMAND DOCS, and improve to include other types
    set string_commands {APPEND BITCOUNT BITFIELD BITOP BITPOS DECR DECRBY GET GETBIT GETRANGE GETSET INCR INCRBY INCRBYFLOAT MGET MSET MSETNX PSETEX SET SETBIT SETEX SETNX SETRANGE LCS STRLEN}
    set hash_commands {HDEL HEXISTS HGET HGETALL HINCRBY HINCRBYFLOAT HKEYS HLEN HMGET HMSET HSCAN HSET HSETNX HSTRLEN HVALS HRANDFIELD}
    set zset_commands {ZADD ZCARD ZCOUNT ZINCRBY ZINTERSTORE ZLEXCOUNT ZPOPMAX ZPOPMIN ZRANGE ZRANGEBYLEX ZRANGEBYSCORE ZRANK ZREM ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANGE ZREVRANGEBYLEX ZREVRANGEBYSCORE ZREVRANK ZSCAN ZSCORE ZUNIONSTORE ZRANDMEMBER}
    set list_commands {LINDEX LINSERT LLEN LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM RPOP RPOPLPUSH RPUSH RPUSHX}
    set set_commands {SADD SCARD SDIFF SDIFFSTORE SINTER SINTERSTORE SISMEMBER SMEMBERS SMOVE SPOP SRANDMEMBER SREM SSCAN SUNION SUNIONSTORE}
    set stream_commands {XACK XADD XCLAIM XDEL XGROUP XINFO XLEN XPENDING XRANGE XREAD XREADGROUP XREVRANGE XTRIM}
    set commands [dict create string $string_commands hash $hash_commands zset $zset_commands list $list_commands set $set_commands stream $stream_commands]

    set type [r type $key]
    set cmds [dict get $commands $type]
    set start_time [clock seconds]
    set sent {}
    set succeeded 0
    while {([clock seconds]-$start_time) < $duration} {
        # find a random command for our key type
        set cmd_idx [expr {int(rand()*[llength $cmds])}]
        set cmd [lindex $cmds $cmd_idx]
        # get the command details from the server
        if { [ catch {
            set cmd_info [lindex [r command info $cmd] 0]
        } err ] } {
            # if we failed, it means the server crashed after the previous command
            return $sent
        }
        # try to build a valid command argument
        set arity [lindex $cmd_info 1]
        set arity [expr $arity < 0 ? - $arity: $arity]
        set firstkey [lindex $cmd_info 3]
        set lastkey [lindex $cmd_info 4]
        set i 1
        if {$cmd == "XINFO"} {
            lappend cmd "STREAM"
            lappend cmd $key
            lappend cmd "FULL"
            incr i 3
        }
        if {$cmd == "XREAD"} {
            lappend cmd "STREAMS"
            lappend cmd $key
            randpath {
                lappend cmd \$
            } {
                lappend cmd [randomValue]
            }
            incr i 3
        }
        if {$cmd == "XADD"} {
            lappend cmd $key
            randpath {
                lappend cmd "*"
            } {
                lappend cmd [randomValue]
            }
            lappend cmd [randomValue]
            lappend cmd [randomValue]
            incr i 4
        }
        for {} {$i < $arity} {incr i} {
            if {$i == $firstkey || $i == $lastkey} {
                lappend cmd $key
            } else {
                lappend cmd [randomValue]
            }
        }
        # execute the command, we expect commands to fail on syntax errors
        lappend sent $cmd
        if { ! [ catch {
            r {*}$cmd
        } err ] } {
            incr succeeded
        } else {
            set err [format "%s" $err] ;# convert to string for pattern matching
            if {[string match "*SIGTERM*" $err]} {
                puts "commands caused test to hang:"
                foreach cmd $sent {
                    foreach arg $cmd {
                        puts -nonewline "[string2printable $arg] "
                    }
                    puts ""
                }
                # Re-raise, let handler up the stack take care of this.
                error $err $::errorInfo
            }
        }
    }

    # print stats so that we know if we managed to generate commands that actually made sense
    #if {$::verbose} {
    #    set count [llength $sent]
    #    puts "Fuzzy traffic sent: $count, succeeded: $succeeded"
    #}

    # return the list of commands we sent
    return $sent
}

proc string2printable s {
    set res {}
    set has_special_chars false
    foreach i [split $s {}] {
        scan $i %c int
        # non printable characters, including space and excluding: " \ $ { }
        if {$int < 32 || $int > 122 || $int == 34 || $int == 36 || $int == 92} {
            set has_special_chars true
        }
        # TCL8.5 has issues mixing \x notation and normal chars in the same
        # source code string, so we'll convert the entire string.
        append res \\x[format %02X $int]
    }
    if {!$has_special_chars} {
        return $s
    }
    set res "\"$res\""
    return $res
}

# Calculation value of Chi-Square Distribution. By this value
# we can verify the random distribution sample confidence.
# Based on the following wiki:
# https://en.wikipedia.org/wiki/Chi-square_distribution
#
# param res    Random sample list
# return       Value of Chi-Square Distribution
#
# x2_value: return of chi_square_value function
# df: Degrees of freedom, Number of independent values minus 1
#
# By using x2_value and df to back check the cardinality table,
# we can know the confidence of the random sample.
proc chi_square_value {res} {
    unset -nocomplain mydict
    foreach key $res {
        dict incr mydict $key 1
    }

    set x2_value 0
    set p [expr [llength $res] / [dict size $mydict]]
    foreach key [dict keys $mydict] {
        set value [dict get $mydict $key]

        # Aggregate the chi-square value of each element
        set v [expr {pow($value - $p, 2) / $p}]
        set x2_value [expr {$x2_value + $v}]
    }

    return $x2_value
}

#subscribe to Pub/Sub channels
proc consume_subscribe_messages {client type channels} {
    set numsub -1
    set counts {}

    for {set i [llength $channels]} {$i > 0} {incr i -1} {
        set msg [$client read]
        assert_equal $type [lindex $msg 0]

        # when receiving subscribe messages the channels names
        # are ordered. when receiving unsubscribe messages
        # they are unordered
        set idx [lsearch -exact $channels [lindex $msg 1]]
        if {[string match "*unsubscribe" $type]} {
            assert {$idx >= 0}
        } else {
            assert {$idx == 0}
        }
        set channels [lreplace $channels $idx $idx]

        # aggregate the subscription count to return to the caller
        lappend counts [lindex $msg 2]
    }

    # we should have received messages for channels
    assert {[llength $channels] == 0}
    return $counts
}

proc subscribe {client channels} {
    $client subscribe {*}$channels
    consume_subscribe_messages $client subscribe $channels
}

proc ssubscribe {client channels} {
    $client ssubscribe {*}$channels
    consume_subscribe_messages $client ssubscribe $channels
}

proc unsubscribe {client {channels {}}} {
    $client unsubscribe {*}$channels
    consume_subscribe_messages $client unsubscribe $channels
}

proc sunsubscribe {client {channels {}}} {
    $client sunsubscribe {*}$channels
    consume_subscribe_messages $client sunsubscribe $channels
}

proc psubscribe {client channels} {
    $client psubscribe {*}$channels
    consume_subscribe_messages $client psubscribe $channels
}

proc punsubscribe {client {channels {}}} {
    $client punsubscribe {*}$channels
    consume_subscribe_messages $client punsubscribe $channels
}

proc debug_digest_value {key} {
    if {[lsearch $::denytags "needs:debug"] >= 0 || $::ignoredigest} {
        return "dummy-digest-value"
    }
    r debug digest-value $key
}

proc debug_digest {{level 0}} {
    if {[lsearch $::denytags "needs:debug"] >= 0 || $::ignoredigest} {
        return "dummy-digest"
    }
    r $level debug digest
}

proc wait_for_blocked_client {{idx 0}} {
    wait_for_condition 50 100 {
        [s $idx blocked_clients] ne 0
    } else {
        fail "no blocked clients"
    }
}

proc wait_for_blocked_clients_count {count {maxtries 100} {delay 10} {idx 0}} {
    wait_for_condition $maxtries $delay  {
        [s $idx blocked_clients] == $count
    } else {
        fail "Timeout waiting for blocked clients"
    }
}

proc wait_for_watched_clients_count {count {maxtries 100} {delay 10} {idx 0}} {
    wait_for_condition $maxtries $delay  {
        [s $idx watching_clients] == $count
    } else {
        fail "Timeout waiting for watched clients"
    }
}

proc read_from_aof {fp} {
    # Input fp is a blocking binary file descriptor of an opened AOF file.
    if {[gets $fp count] == -1} return ""
    set count [string range $count 1 end]

    # Return a list of arguments for the command.
    set res {}
    for {set j 0} {$j < $count} {incr j} {
        read $fp 1
        set arg [::valkey::valkey_bulk_read $fp]
        if {$j == 0} {set arg [string tolower $arg]}
        lappend res $arg
    }
    return $res
}

proc assert_aof_content {aof_path patterns} {
    set fp [open $aof_path r]
    fconfigure $fp -translation binary
    fconfigure $fp -blocking 1

    for {set j 0} {$j < [llength $patterns]} {incr j} {
        assert_match [lindex $patterns $j] [read_from_aof $fp]
    }
}

proc config_set {param value {options {}}} {
    set mayfail 0
    foreach option $options {
        switch $option {
            "mayfail" {
                set mayfail 1
            }
            default {
                error "Unknown option $option"
            }
        }
    }

    if {[catch {r config set $param $value} err]} {
        if {!$mayfail} {
            error $err
        } else {
            if {$::verbose} {
                puts "Ignoring CONFIG SET $param $value failure: $err"
            }
        }
    }
}

proc config_get_set {param value {options {}}} {
    set config [lindex [r config get $param] 1]
    config_set $param $value $options
    return $config
}

proc delete_lines_with_pattern {filename tmpfilename pattern} {
    set fh_in [open $filename r]
    set fh_out [open $tmpfilename w]
    while {[gets $fh_in line] != -1} {
        if {![regexp $pattern $line]} {
            puts $fh_out $line
        }
    }
    close $fh_in
    close $fh_out
    file rename -force $tmpfilename $filename
}

proc get_nonloopback_addr {} {
    set addrlist [list {}]
    catch { set addrlist [exec hostname -I] }
    return [lindex $addrlist 0]
}

proc get_nonloopback_client {} {
    return [valkey [get_nonloopback_addr] [srv 0 "port"] 0 $::tls]
}

# The following functions and variables are used only when running large-memory
# tests. We avoid defining them when not running large-memory tests because the 
# global variables takes up lots of memory.
proc init_large_mem_vars {} {
    if {![info exists ::str500]} {
        set ::str500 [string repeat x 500000000] ;# 500mb
        set ::str500_len [string length $::str500]
    }
}

# Utility function to write big argument into a server client connection
proc write_big_bulk {size {prefix ""} {skip_read no}} {
    init_large_mem_vars

    assert {[string length prefix] <= $size}
    r write "\$$size\r\n"
    r write $prefix
    incr size -[string length $prefix]
    while {$size >= 500000000} {
        r write $::str500
        incr size -500000000
    }
    if {$size > 0} {
        r write [string repeat x $size]
    }
    r write "\r\n"
    if {!$skip_read} {
        r flush
        r read
    }
}

# Utility to read big bulk response (work around Tcl limitations)
proc read_big_bulk {code {compare no} {prefix ""}} {
    init_large_mem_vars

    r readraw 1
    set resp_len [uplevel 1 $code] ;# get the first line of the RESP response
    assert_equal [string range $resp_len 0 0] "$"
    set resp_len [string range $resp_len 1 end]
    set prefix_len [string length $prefix]
    if {$compare} {
        assert {$prefix_len <= $resp_len}
        assert {$prefix_len <= $::str500_len}
    }

    set remaining $resp_len
    while {$remaining > 0} {
        set l $remaining
        if {$l > $::str500_len} {set l $::str500_len} ; # can't read more than 2gb at a time, so read 500mb so we can easily verify read data
        set read_data [r rawread $l]
        set nbytes [string length $read_data]
        if {$compare} {
            set comp_len $nbytes
            # Compare prefix part
            if {$remaining == $resp_len} {
                assert_equal $prefix [string range $read_data 0 [expr $prefix_len - 1]]
                set read_data [string range $read_data $prefix_len $nbytes]
                incr comp_len -$prefix_len
            }
            # Compare rest of data, evaluate and then assert to avoid huge print in case of failure
            set data_equal [expr {$read_data == [string range $::str500 0 [expr $comp_len - 1]]}]
            assert $data_equal
        }
        incr remaining -$nbytes
    }
    assert_equal [r rawread 2] "\r\n"
    r readraw 0
    return $resp_len
}

proc prepare_value {size} {
    set _v "c"
    for {set i 1} {$i < $size} {incr i} {
        append _v 0
    }
    return $_v
}

proc memory_usage {key} {
    set usage [r memory usage $key]
    if {![string match {*jemalloc*} [s mem_allocator]]} {
        # libc allocator can sometimes return a different size allocation for the same requested size
        # this makes tests that rely on MEMORY USAGE unreliable, so instead we return a constant 1
        set usage 1
    }
    return $usage
}

# forward compatibility, lmap missing in TCL 8.5
proc lmap args {
    set body [lindex $args end]
    set args [lrange $args 0 end-1]
    set n 0
    set pairs [list]
    foreach {varnames listval} $args {
        set varlist [list]
        foreach varname $varnames {
            upvar 1 $varname var$n
            lappend varlist var$n
            incr n
        }
        lappend pairs $varlist $listval
    }
    set temp [list]
    foreach {*}$pairs {
        lappend temp [uplevel 1 $body]
    }
    set temp
}

proc format_command {args} {
    set cmd "*[llength $args]\r\n"
    foreach a $args {
        append cmd "$[string length $a]\r\n$a\r\n"
    }
    set _ $cmd
}

# Returns whether or not the system supports stack traces
proc system_backtrace_supported {} {
    set system_name [string tolower [exec uname -s]]
    if {$system_name eq {darwin}} {
        return 1
    } elseif {$system_name ne {linux}} {
        return 0
    }

    # libmusl does not support backtrace. Also return 0 on
    # static binaries (ldd exit code 1) where we can't detect libmusl
    catch {
        set ldd [exec ldd src/valkey-server]
        if {![string match {*libc.*musl*} $ldd]} {
            return 1
        }
    }
    return 0
}

proc generate_largevalue_test_array {} {
    array set largevalue {}
    set largevalue(listpack) "hello"
    set largevalue(quicklist) [string repeat "x" 8192]
    return [array get largevalue]
}

# Breakpoint function, which invokes a minimal debugger.
# This function can be placed within the desired Tcl tests for debugging purposes.
# 
# Arguments:
# * 's': breakpoint label, which is printed when breakpoints are hit for unique identification.
# 
# Source: https://wiki.tcl-lang.org/page/A+minimal+debugger
proc bp {{s {}}} {
    if ![info exists ::bp_skip] {
        set ::bp_skip [list]
    } elseif {[lsearch -exact $::bp_skip $s]>=0} return
    if [catch {info level -1} who] {set who ::}
    while 1 {
        puts -nonewline "$who/$s> "; flush stdout
        gets stdin line
        if {$line=="c"} {puts "continuing.."; break}
        if {$line=="i"} {set line "info locals"}
        catch {uplevel 1 $line} res
        puts $res
    }
}


set dir [pwd]
set ::all_tests []
set ::cluster_all_test []
set ::module_api_all_tests []

set test_dirs {
    unit
    unit/type
    unit/cluster
    integration
}

foreach test_dir $test_dirs {
    set files [glob -nocomplain $dir/$test_dir/*.tcl]

    foreach file $files {
        lappend ::all_tests $test_dir/[file root [file tail $file]]
    }
}

set cluster_test_dir unit/cluster
foreach file [glob -nocomplain $dir/$cluster_test_dir/*.tcl] {
    lappend ::cluster_all_tests $cluster_test_dir/[file root [file tail $file]]
}

set moduleapi_test_dir unit/moduleapi
foreach file [glob -nocomplain $dir/$moduleapi_test_dir/*.tcl] {
    lappend ::module_api_all_tests $moduleapi_test_dir/[file root [file tail $file]]
}

# Index to the next test to run in the ::all_tests list.
set ::next_test 0

set ::host 127.0.0.1
set ::port 6379; # port for external server
set ::baseport 21111; # initial port for spawned servers
set ::portcount 8000; # we don't wanna use more than 10000 to avoid collision with cluster bus ports
set ::traceleaks 0
set ::valgrind 0
set ::durable 0
set ::tls 0
set ::io_threads 0
set ::tls_module 0
set ::stack_logging 0
set ::verbose 0
set ::quiet 0
set ::denytags {}
set ::skiptests {}
set ::skipunits {}
set ::no_latency 0
set ::allowtags {}
set ::only_tests {}
set ::single_tests {}
set ::run_solo_tests {}
set ::skip_till ""
set ::external 0; # If "1" this means, we are running against external instance
set ::file ""; # If set, runs only the tests in this comma separated list
set ::curfile ""; # Hold the filename of the current suite
set ::accurate 0; # If true runs fuzz tests with more iterations
set ::force_failure 0
set ::timeout 1200; # 20 minutes without progresses will quit the test.
set ::last_progress [clock seconds]
set ::active_servers {} ; # Pids of active server instances.
set ::dont_clean 0
set ::dont_pre_clean 0
set ::wait_server 0
set ::exit_on_failure 0
set ::stop_on_failure 0
set ::dump_logs 0
set ::loop 0
set ::tlsdir "tls"
set ::singledb 0
set ::cluster_mode 0
set ::ignoreencoding 0
set ::ignoredigest 0
set ::large_memory 0
set ::log_req_res 0
set ::force_resp3 0
set ::solo_tests_count 0

# Set to 1 when we are running in client mode. The server test uses a
# server-client model to run tests simultaneously. The server instance
# runs the specified number of client instances that will actually run tests.
# The server is responsible of showing the result to the user, and exit with
# the appropriate exit code depending on the test outcome.
set ::client 0
set ::numclients 16

# This function is called by one of the test clients when it receives
# a "run" command from the server, with a filename as data.
# It will run the specified test source file and signal it to the
# test server when finished.
proc execute_test_file __testname {
    set path "$__testname.tcl"
    set ::curfile $path
    source $path
    send_data_packet $::test_server_fd done "$__testname"
}

# This function is called by one of the test clients when it receives
# a "run_code" command from the server, with a verbatim test source code
# as argument, and an associated name.
# It will run the specified code and signal it to the test server when
# finished.
proc execute_test_code {__testname filename code} {
    set ::curfile $filename
    eval $code
    send_data_packet $::test_server_fd done "$__testname"
}

# Setup a list to hold a stack of server configs. When calls to start_server
# are nested, use "srv 0 pid" to get the pid of the inner server. To access
# outer servers, use "srv -1 pid" etcetera.
set ::servers {}
proc srv {args} {
    set level 0
    if {[string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set property [lindex $args 1]
    } else {
        set property [lindex $args 0]
    }
    set srv [lindex $::servers end+$level]
    dict get $srv $property
}

# Take an index to get a srv.
proc get_srv {level} {
    set srv [lindex $::servers end+$level]
    return $srv
}

# Provide easy access to the client for the inner server. It's possible to
# prepend the argument list with a negative level to access clients for
# servers running in outer blocks.
proc r {args} {
    set level 0
    if {[string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }
    [srv $level "client"] {*}$args
}

# Returns a server instance by index.
proc Rn {n} {
    set level [expr -1*$n]
    return [srv $level "client"]
}

# Provide easy access to a client for an inner server. Requires a positive
# index, unlike r which uses an optional negative index.
proc R {n args} {
    [Rn $n] {*}$args
}

proc reconnect {args} {
    set level [lindex $args 0]
    if {[string length $level] == 0 || ![string is integer $level]} {
        set level 0
    }

    set srv [lindex $::servers end+$level]
    set host [dict get $srv "host"]
    set port [dict get $srv "port"]
    set config [dict get $srv "config"]
    set client [valkey $host $port 0 $::tls]
    if {[dict exists $srv "client"]} {
        set old [dict get $srv "client"]
        $old close
    }
    dict set srv "client" $client

    # select the right db when we don't have to authenticate
    if {![dict exists $config "requirepass"] && !$::singledb} {
        $client select 9
    }

    # re-set $srv in the servers list
    lset ::servers end+$level $srv
}

proc valkey_deferring_client {args} {
    set level 0
    if {[llength $args] > 0 && [string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }

    # create client that defers reading reply
    set client [valkey [srv $level "host"] [srv $level "port"] 1 $::tls]

    # select the right db and read the response (OK)
    if {!$::singledb} {
        $client select 9
        $client read
    } else {
        # For timing/symmetry with the above select
        $client ping
        $client read
    }
    return $client
}

proc valkey_client {args} {
    set level 0
    if {[llength $args] > 0 && [string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }

    # create client that won't defers reading reply
    set client [valkey [srv $level "host"] [srv $level "port"] 0 $::tls]

    # select the right db and read the response (OK), or at least ping
    # the server if we're in a singledb mode.
    if {$::singledb} {
        $client ping
    } else {
        $client select 9
    }
    return $client
}

proc valkey_deferring_client_by_addr {host port} {
    set client [valkey $host $port 1 $::tls]
    return $client
}

proc valkey_client_by_addr {host port} {
    set client [valkey $host $port 0 $::tls]
    return $client
}

# Provide easy access to INFO properties. Same semantic as "proc r".
proc s {args} {
    set level 0
    if {[string is integer [lindex $args 0]]} {
        set level [lindex $args 0]
        set args [lrange $args 1 end]
    }
    status [srv $level "client"] [lindex $args 0]
}

# Get the specified field from the givens instances cluster info output.
proc CI {index field} {
    getInfoProperty [R $index cluster info] $field
}

# Test wrapped into run_solo are sent back from the client to the
# test server, so that the test server will send them again to
# clients once the clients are idle.
proc run_solo {name code} {
    if {$::numclients == 1 || $::loop || $::external} {
        # run_solo is not supported in these scenarios, just run the code.
        eval $code
        return
    }
    send_data_packet $::test_server_fd run_solo [list $name $::curfile $code]
}

proc cleanup {} {
    if {!$::quiet} {puts -nonewline "Cleanup: may take some time... "}
    flush stdout
    catch {exec rm -rf {*}[glob tmp/valkey.conf.*]}
    catch {exec rm -rf {*}[glob tmp/server*.*]}
    catch {exec rm -rf {*}[glob tmp/*.acl.*]}
    if {!$::quiet} {puts "OK"}
}

proc test_server_main {} {
    if {!$::dont_pre_clean} cleanup
    set tclsh [info nameofexecutable]
    # Open a listening socket, trying different ports in order to find a
    # non busy one.
    set clientport [find_available_port [expr {$::baseport - 32}] 32]
    if {!$::quiet} {
        puts "Starting test server at port $clientport"
    }
    socket -server accept_test_clients  -myaddr 127.0.0.1 $clientport

    # Start the client instances
    set ::clients_pids {}
    if {$::external} {
        set p [exec $tclsh [info script] {*}$::argv \
            --client $clientport &]
        lappend ::clients_pids $p
    } else {
        set start_port $::baseport
        set port_count [expr {$::portcount / $::numclients}]
        for {set j 0} {$j < $::numclients} {incr j} {
            set p [exec $tclsh [info script] {*}$::argv \
                --client $clientport --baseport $start_port --portcount $port_count &]
            lappend ::clients_pids $p
            incr start_port $port_count
        }
    }

    # Setup global state for the test server
    set ::idle_clients {}
    set ::active_clients {}
    array set ::active_clients_task {}
    array set ::clients_start_time {}
    set ::clients_time_history {}
    set ::failed_tests {}

    # Enter the event loop to handle clients I/O
    after 100 test_server_cron
    vwait forever
}

# This function gets called 10 times per second.
proc test_server_cron {} {
    set elapsed [expr {[clock seconds]-$::last_progress}]

    if {$elapsed > $::timeout} {
        set err "\[[colorstr red TIMEOUT]\]: clients state report follows."
        puts $err
        lappend ::failed_tests $err
        show_clients_state
        kill_clients
        force_kill_all_servers
        the_end
    }

    after 100 test_server_cron
}

proc accept_test_clients {fd addr port} {
    fconfigure $fd -encoding binary
    fileevent $fd readable [list read_from_test_client $fd]
}

# This is the readable handler of our test server. Clients send us messages
# in the form of a status code such and additional data. Supported
# status types are:
#
# ready: the client is ready to execute the command. Only sent at client
#        startup. The server will queue the client FD in the list of idle
#        clients.
# testing: just used to signal that a given test started.
# ok: a test was executed with success.
# err: a test was executed with an error.
# skip: a test was skipped by skipfile or individual test options.
# ignore: a test was skipped by a group tag.
# exception: there was a runtime exception while executing the test.
# done: all the specified test file was processed, this test client is
#       ready to accept a new task.
proc read_from_test_client fd {
    set bytes [gets $fd]
    set payload [read $fd $bytes]
    foreach {status data elapsed} $payload break
    set ::last_progress [clock seconds]

    if {$status eq {ready}} {
        if {!$::quiet} {
            puts "\[$status\]: $data"
        }
        signal_idle_client $fd
    } elseif {$status eq {done}} {
        set elapsed [expr {[clock seconds]-$::clients_start_time($fd)}]
        set all_tests_count [expr {[llength $::all_tests]+$::solo_tests_count}]
        set running_tests_count [expr {[llength $::active_clients]-1}]
        set completed_solo_tests_count [expr {$::solo_tests_count-[llength $::run_solo_tests]}]
        set completed_tests_count [expr {$::next_test-$running_tests_count+$completed_solo_tests_count}]
        puts "\[$completed_tests_count/$all_tests_count [colorstr yellow $status]\]: $data ($elapsed seconds)"
        lappend ::clients_time_history $elapsed $data
        signal_idle_client $fd
        set ::active_clients_task($fd) "(DONE) $data"
    } elseif {$status eq {ok}} {
        if {!$::quiet} {
            puts "\[[colorstr green $status]\]: $data ($elapsed ms)"
        }
        set ::active_clients_task($fd) "(OK) $data"
    } elseif {$status eq {skip}} {
        if {!$::quiet} {
            puts "\[[colorstr yellow $status]\]: $data"
        }
    } elseif {$status eq {ignore}} {
        if {!$::quiet} {
            puts "\[[colorstr cyan $status]\]: $data"
        }
    } elseif {$status eq {err}} {
        set err "\[[colorstr red $status]\]: $data"
        puts $err
        lappend ::failed_tests $err
        set ::active_clients_task($fd) "(ERR) $data"
        if {$::exit_on_failure} {
            puts -nonewline "(Fast fail: test will exit now)"
            flush stdout
            exit 1
        }
        if {$::stop_on_failure} {
            puts -nonewline "(Test stopped, press enter to resume the tests)"
            flush stdout
            gets stdin
        }
    } elseif {$status eq {exception}} {
        puts "\[[colorstr red $status]\]: $data"
        kill_clients
        force_kill_all_servers
        exit 1
    } elseif {$status eq {testing}} {
        set ::active_clients_task($fd) "(IN PROGRESS) $data"
    } elseif {$status eq {server-spawning}} {
        set ::active_clients_task($fd) "(SPAWNING SERVER) $data"
    } elseif {$status eq {server-spawned}} {
        lappend ::active_servers $data
        set ::active_clients_task($fd) "(SPAWNED SERVER) pid:$data"
    } elseif {$status eq {server-killing}} {
        set ::active_clients_task($fd) "(KILLING SERVER) pid:$data"
    } elseif {$status eq {server-killed}} {
        set ::active_servers [lsearch -all -inline -not -exact $::active_servers $data]
        set ::active_clients_task($fd) "(KILLED SERVER) pid:$data"
    } elseif {$status eq {run_solo}} {
        lappend ::run_solo_tests $data
        incr ::solo_tests_count
    } else {
        if {!$::quiet} {
            puts "\[$status\]: $data"
        }
    }
}

proc show_clients_state {} {
    # The following loop is only useful for debugging tests that may
    # enter an infinite loop.
    foreach x $::active_clients {
        if {[info exist ::active_clients_task($x)]} {
            puts "$x => $::active_clients_task($x)"
        } else {
            puts "$x => ???"
        }
    }
}

proc kill_clients {} {
    foreach p $::clients_pids {
        catch {exec kill $p}
    }
}

proc force_kill_all_servers {} {
    foreach p $::active_servers {
        puts "Killing still running Valkey server $p"
        catch {exec kill -9 $p}
    }
}

proc lpop {listVar {count 1}} {
    upvar 1 $listVar l
    set ele [lindex $l 0]
    set l [lrange $l 1 end]
    set ele
}

proc lremove {listVar value} {
    upvar 1 $listVar var
    set idx [lsearch -exact $var $value]
    set var [lreplace $var $idx $idx]
}

# A new client is idle. Remove it from the list of active clients and
# if there are still test units to run, launch them.
proc signal_idle_client fd {
    # Remove this fd from the list of active clients.
    set ::active_clients \
        [lsearch -all -inline -not -exact $::active_clients $fd]

    # New unit to process?
    if {$::next_test != [llength $::all_tests]} {
        if {!$::quiet} {
            puts [colorstr bold-white "Testing [lindex $::all_tests $::next_test]"]
            set ::active_clients_task($fd) "ASSIGNED: $fd ([lindex $::all_tests $::next_test])"
        }
        set ::clients_start_time($fd) [clock seconds]
        send_data_packet $fd run [lindex $::all_tests $::next_test]
        lappend ::active_clients $fd
        incr ::next_test
        if {$::loop > 1 && $::next_test == [llength $::all_tests]} {
            set ::next_test 0
            incr ::loop -1
        }
    } elseif {[llength $::run_solo_tests] != 0 && [llength $::active_clients] == 0} {
        if {!$::quiet} {
            puts [colorstr bold-white "Testing solo test"]
            set ::active_clients_task($fd) "ASSIGNED: $fd solo test"
        }
        set ::clients_start_time($fd) [clock seconds]
        send_data_packet $fd run_code [lpop ::run_solo_tests]
        lappend ::active_clients $fd
    } else {
        lappend ::idle_clients $fd
        set ::active_clients_task($fd) "SLEEPING, no more units to assign"
        if {[llength $::active_clients] == 0} {
            the_end
        }
    }
}

# The the_end function gets called when all the test units were already
# executed, so the test finished.
proc the_end {} {
    # TODO: print the status, exit with the right exit code.
    puts "\n                   The End\n"
    puts "Execution time of different units:"
    foreach {time name} $::clients_time_history {
        puts "  $time seconds - $name"
    }
    if {[llength $::failed_tests]} {
        puts "\n[colorstr bold-red {!!! WARNING}] The following tests failed:\n"
        foreach failed $::failed_tests {
            puts "*** $failed"
        }
        if {!$::dont_clean} cleanup
        exit 1
    } else {
        puts "\n[colorstr bold-white {\o/}] [colorstr bold-green {All tests passed without errors!}]\n"
        if {!$::dont_clean} cleanup
        exit 0
    }
}

# The client is not event driven (the test server is instead) as we just need
# to read the command, execute, reply... all this in a loop.
proc test_client_main server_port {
    set ::test_server_fd [socket localhost $server_port]
    fconfigure $::test_server_fd -encoding binary
    send_data_packet $::test_server_fd ready [pid]
    while 1 {
        set bytes [gets $::test_server_fd]
        set payload [read $::test_server_fd $bytes]
        foreach {cmd data} $payload break
        if {$cmd eq {run}} {
            execute_test_file $data
        } elseif {$cmd eq {run_code}} {
            foreach {name filename code} $data break
            execute_test_code $name $filename $code
        } else {
            error "Unknown test client command: $cmd"
        }
    }
}

proc send_data_packet {fd status data {elapsed 0}} {
    set payload [list $status $data $elapsed]
    puts $fd [string length $payload]
    puts -nonewline $fd $payload
    flush $fd
}

proc print_help_screen {} {
    puts [join {
        "--cluster          Run the cluster tests, by default cluster tests run along with all tests."
        "--moduleapi        Run the module API tests, this option should only be used in runtest-moduleapi which will build the test module."
        "--valgrind         Run the test over valgrind."
        "--durable          suppress test crashes and keep running"
        "--stack-logging    Enable OSX leaks/malloc stack logging."
        "--accurate         Run slow randomized tests for more iterations."
        "--quiet            Don't show individual tests."
        "--single <unit>    Just execute the specified unit (see next option). This option can be repeated."
        "--verbose          Increases verbosity."
        "--list-tests       List all the available test units."
        "--only <test>      Just execute the specified test by test name or tests that match <test> regexp (if <test> starts with '/'). This option can be repeated."
        "--skip-till <unit> Skip all units until (and including) the specified one."
        "--skipunit <unit>  Skip one unit."
        "--clients <num>    Number of test clients (default 16)."
        "--timeout <sec>    Test timeout in seconds (default 20 min)."
        "--force-failure    Force the execution of a test that always fails."
        "--config <k> <v>   Extra config file argument."
        "--skipfile <file>  Name of a file containing test names or regexp patterns (if <test> starts with '/') that should be skipped (one per line). This option can be repeated."
        "--skiptest <test>  Test name or regexp pattern (if <test> starts with '/') to skip. This option can be repeated."
        "--tags <tags>      Run only tests having specified tags or not having '-' prefixed tags."
        "--dont-clean       Don't delete valkey log files after the run."
        "--dont-pre-clean   Don't delete existing valkey log files before the run."
        "--no-latency       Skip latency measurements and validation by some tests."
        "--fastfail         Exit immediately once the first test fails."
        "--stop             Blocks once the first test fails."
        "--loop             Execute the specified set of tests forever."
        "--loops <count>    Execute the specified set of tests several times."
        "--wait-server      Wait after server is started (so that you can attach a debugger)."
        "--dump-logs        Dump server log on test failure."
        "--io-threads       Run tests with IO threads."
        "--tls              Run tests in TLS mode."
        "--tls-module       Run tests in TLS mode with Valkey module."
        "--host <addr>      Run tests against an external host."
        "--port <port>      TCP port to use against external host."
        "--baseport <port>  Initial port number for spawned valkey servers."
        "--portcount <num>  Port range for spawned valkey servers."
        "--singledb         Use a single database, avoid SELECT."
        "--cluster-mode     Run tests in cluster protocol compatible mode."
        "--ignore-encoding  Don't validate object encoding."
        "--ignore-digest    Don't use debug digest validations."
        "--large-memory     Run tests using over 100mb."
        "--help             Print this help screen."
    } "\n"]
}

# parse arguments
for {set j 0} {$j < [llength $argv]} {incr j} {
    set opt [lindex $argv $j]
    set arg [lindex $argv [expr $j+1]]
    if {$opt eq {--tags}} {
        foreach tag $arg {
            if {[string index $tag 0] eq "-"} {
                lappend ::denytags [string range $tag 1 end]
            } else {
                lappend ::allowtags $tag
            }
        }
        incr j
    } elseif {$opt eq {--cluster}} {
        set ::all_tests $::cluster_all_tests
    } elseif {$opt eq {--moduleapi}} {
        set ::all_tests $::module_api_all_tests
    } elseif {$opt eq {--config}} {
        set arg2 [lindex $argv [expr $j+2]]
        lappend ::global_overrides $arg
        lappend ::global_overrides $arg2
        incr j 2
    } elseif {$opt eq {--log-req-res}} {
        set ::log_req_res 1
    } elseif {$opt eq {--force-resp3}} {
        set ::force_resp3 1
    } elseif {$opt eq {--skipfile}} {
        incr j
        set fp [open $arg r]
        set file_data [read $fp]
        close $fp
        set ::skiptests [concat $::skiptests [split $file_data "\n"]]
    } elseif {$opt eq {--skiptest}} {
        lappend ::skiptests $arg
        incr j
    } elseif {$opt eq {--valgrind}} {
        set ::valgrind 1
    } elseif {$opt eq {--stack-logging}} {
        if {[string match {*Darwin*} [exec uname -a]]} {
            set ::stack_logging 1
        }
    } elseif {$opt eq {--quiet}} {
        set ::quiet 1
    } elseif {$opt eq {--io-threads}} {
        set ::io_threads 1
    } elseif {$opt eq {--tls} || $opt eq {--tls-module}} {
        package require tls 1.6
        set ::tls 1
        ::tls::init \
            -cafile "$::tlsdir/ca.crt" \
            -certfile "$::tlsdir/client.crt" \
            -keyfile "$::tlsdir/client.key"
        if {$opt eq {--tls-module}} {
            set ::tls_module 1
        }
    } elseif {$opt eq {--host}} {
        set ::external 1
        set ::host $arg
        incr j
    } elseif {$opt eq {--port}} {
        set ::port $arg
        incr j
    } elseif {$opt eq {--baseport}} {
        set ::baseport $arg
        incr j
    } elseif {$opt eq {--portcount}} {
        set ::portcount $arg
        incr j
    } elseif {$opt eq {--accurate}} {
        set ::accurate 1
    } elseif {$opt eq {--force-failure}} {
        set ::force_failure 1
    } elseif {$opt eq {--single}} {
        lappend ::single_tests $arg
        incr j
    } elseif {$opt eq {--only}} {
        lappend ::only_tests $arg
        incr j
    } elseif {$opt eq {--skipunit}} {
        lappend ::skipunits $arg
        incr j
    } elseif {$opt eq {--skip-till}} {
        set ::skip_till $arg
        incr j
    } elseif {$opt eq {--list-tests}} {
        foreach t $::all_tests {
            puts $t
        }
        exit 0
    } elseif {$opt eq {--verbose}} {
        incr ::verbose
    } elseif {$opt eq {--client}} {
        set ::client 1
        set ::test_server_port $arg
        incr j
    } elseif {$opt eq {--clients}} {
        set ::numclients $arg
        incr j
    } elseif {$opt eq {--durable}} {
        set ::durable 1
    } elseif {$opt eq {--dont-clean}} {
        set ::dont_clean 1
    } elseif {$opt eq {--dont-pre-clean}} {
        set ::dont_pre_clean 1
    } elseif {$opt eq {--no-latency}} {
        set ::no_latency 1
    } elseif {$opt eq {--wait-server}} {
        set ::wait_server 1
    } elseif {$opt eq {--dump-logs}} {
        set ::dump_logs 1
    } elseif {$opt eq {--fastfail}} {
        set ::exit_on_failure 1
    } elseif {$opt eq {--stop}} {
        set ::stop_on_failure 1
    } elseif {$opt eq {--loop}} {
        set ::loop 2147483647
    } elseif {$opt eq {--loops}} {
        set ::loop $arg
        if {$::loop <= 0} {
            puts "Wrong argument: $opt, loops should be greater than 0"
            exit 1
        }
        incr j
    } elseif {$opt eq {--timeout}} {
        set ::timeout $arg
        incr j
    } elseif {$opt eq {--singledb}} {
        set ::singledb 1
    } elseif {$opt eq {--cluster-mode}} {
        set ::cluster_mode 1
        set ::singledb 1
    } elseif {$opt eq {--large-memory}} {
        set ::large_memory 1
    } elseif {$opt eq {--ignore-encoding}} {
        set ::ignoreencoding 1
    } elseif {$opt eq {--ignore-digest}} {
        set ::ignoredigest 1
    } elseif {$opt eq {--help}} {
        print_help_screen
        exit 0
    } else {
        puts "Wrong argument: $opt"
        exit 1
    }
}

set filtered_tests {}

# Set the filtered tests to be the short list (single_tests) if exists.
# Otherwise, we start filtering all_tests
if {[llength $::single_tests] > 0} {
    set filtered_tests $::single_tests
} else {
    set filtered_tests $::all_tests
}

# If --skip-till option was given, we populate the list of single tests
# to run with everything *after* the specified unit.
if {$::skip_till != ""} {
    set skipping 1
    foreach t $::all_tests {
        if {$skipping == 1} {
            lremove filtered_tests $t
        }
        if {$t == $::skip_till} {
            set skipping 0
        }
    }
    if {$skipping} {
        puts "test $::skip_till not found"
        exit 0
    }
}

# If --skipunits option was given, we populate the list of single tests
# to run with everything *not* in the skipunits list.
if {[llength $::skipunits] > 0} {
    foreach t $::all_tests {
        if {[lsearch $::skipunits $t] != -1} {
            lremove filtered_tests $t
        }
    }
}

# Override the list of tests with the specific tests we want to run
# in case there was some filter, that is --single, -skipunit or --skip-till options.
if {[llength $filtered_tests] < [llength $::all_tests]} {
    set ::all_tests $filtered_tests
}

proc attach_to_replication_stream_on_connection {conn} {
    r config set repl-ping-replica-period 3600
    if {$::tls} {
        set s [::tls::socket [srv $conn "host"] [srv $conn "port"]]
    } else {
        set s [socket [srv $conn "host"] [srv $conn "port"]]
    }
    fconfigure $s -translation binary
    puts -nonewline $s "SYNC\r\n"
    flush $s

    # Get the count
    while 1 {
        set count [gets $s]
        set prefix [string range $count 0 0]
        if {$prefix ne {}} break; # Newlines are allowed as PINGs.
    }
    if {$prefix ne {$}} {
        error "attach_to_replication_stream error. Received '$count' as count."
    }
    set count [string range $count 1 end]

    # Consume the bulk payload
    while {$count} {
        set buf [read $s $count]
        set count [expr {$count-[string length $buf]}]
    }
    return $s
}

proc attach_to_replication_stream {} {
    return [attach_to_replication_stream_on_connection 0]
}

proc read_from_replication_stream {s} {
    fconfigure $s -blocking 0
    set attempt 0
    while {[gets $s count] == -1} {
        if {[incr attempt] == 10} return ""
        after 100
    }
    fconfigure $s -blocking 1
    set count [string range $count 1 end]

    # Return a list of arguments for the command.
    set res {}
    for {set j 0} {$j < $count} {incr j} {
        read $s 1
        set arg [::valkey::valkey_bulk_read $s]
        if {$j == 0} {set arg [string tolower $arg]}
        lappend res $arg
    }
    return $res
}

proc assert_replication_stream {s patterns} {
    set errors 0
    set values_list {}
    set patterns_list {}
    for {set j 0} {$j < [llength $patterns]} {incr j} {
        set pattern [lindex $patterns $j]
        lappend patterns_list $pattern
        set value [read_from_replication_stream $s]
        lappend values_list $value
        if {![string match $pattern $value]} { incr errors }
    }

    if {$errors == 0} { return }

    set context [info frame -1]
    close_replication_stream $s ;# for fast exit
    assert_match $patterns_list $values_list "" $context
}

proc close_replication_stream {s} {
    close $s
    r config set repl-ping-replica-period 10
    return
}

# With the parallel test running multiple server instances at the same time
# we need a fast enough computer, otherwise a lot of tests may generate
# false positives.
# If the computer is too slow we revert the sequential test without any
# parallelism, that is, clients == 1.
proc is_a_slow_computer {} {
    set start [clock milliseconds]
    for {set j 0} {$j < 1000000} {incr j} {}
    set elapsed [expr [clock milliseconds]-$start]
    expr {$elapsed > 200}
}

if {$::client} {
    if {[catch { test_client_main $::test_server_port } err]} {
        set estr "Executing test client: $err.\n$::errorInfo"
        if {[catch {send_data_packet $::test_server_fd exception $estr}]} {
            puts $estr
        }
        exit 1
    }
} else {
    if {[is_a_slow_computer]} {
        puts "** SLOW COMPUTER ** Using a single client to avoid false positives."
        set ::numclients 1
    }

    if {[catch { test_server_main } err]} {
        if {[string length $err] > 0} {
            # only display error when not generated by the test suite
            if {$err ne "exception"} {
                puts $::errorInfo
            }
            exit 1
        }
    }
}
