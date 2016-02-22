## Copyright (c) 2012-2016, Robert Escriva
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of Replicant nor the names of its contributors may be
##       used to endorse or promote products derived from this software without
##       specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

AM_CPPFLAGS  = -DREPLICANT_EXEC_DIR="\"$(replicantexecdir)\"" -I${abs_top_srcdir}/include $(SPARSEHASH_CFLAGS) $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS)
AM_CFLAGS    = -fvisibility=hidden $(WANAL_CFLAGS)
AM_CXXFLAGS  = -fvisibility=hidden -fvisibility-inlines-hidden $(SPARSEHASH_CFLAGS) $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
HELP2MAN_FLAGS = --no-discard-stderr --libtool --no-info --version-string=$(VERSION) --manual="Replicant User Manual"
TESTS_ENVIRONMENT = . $(abs_top_srcdir)/test/env.sh "${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";
AM_DISTCHECK_CONFIGURE_FLAGS = --enable-example-state-machines

replicantexecdir = $(libexecdir)/$(PACKAGE)-$(VERSION)
pkgconfigdir = $(libdir)/pkgconfig

BUILT_SOURCES =

EXTRA_DIST =

CLEANFILES =

include_HEADERS =
noinst_HEADERS =

lib_LTLIBRARIES =
noinst_LTLIBRARIES =

bin_PROGRAMS =
noinst_PROGRAMS =
EXTRA_PROGRAMS =

check_SCRIPTS =

replicantexec_PROGRAMS =
replicantexec_LTLIBRARIES =

pkgconfig_DATA =

TESTS =

################################################################################
#################################### Common ####################################
################################################################################

EXTRA_DIST += LICENSE

noinst_HEADERS += namespace.h
noinst_HEADERS += visibility.h
noinst_HEADERS += common/atomic_io.h
noinst_HEADERS += common/bootstrap.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/constants.h
noinst_HEADERS += common/generate_token.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/packing.h
noinst_HEADERS += common/quorum_calc.h
noinst_HEADERS += common/server.h

################################################################################
#################################### Daemon ####################################
################################################################################

replicantexec_PROGRAMS += replicant-daemon

noinst_HEADERS += daemon/acceptor.h
noinst_HEADERS += daemon/ballot.h
noinst_HEADERS += daemon/commander.h
noinst_HEADERS += daemon/condition.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/deferred_msg.h
noinst_HEADERS += daemon/failure_tracker.h
noinst_HEADERS += daemon/leader.h
noinst_HEADERS += daemon/mapper.h
noinst_HEADERS += daemon/object.h
noinst_HEADERS += daemon/object_interface.h
noinst_HEADERS += daemon/pvalue.h
noinst_HEADERS += daemon/replica.h
noinst_HEADERS += daemon/robust_history.h
noinst_HEADERS += daemon/rsm.h
noinst_HEADERS += daemon/scout.h
noinst_HEADERS += daemon/settings.h
noinst_HEADERS += daemon/slot_type.h
noinst_HEADERS += daemon/snapshot.h
noinst_HEADERS += daemon/unordered_command.h

replicant_daemon_SOURCES =
replicant_daemon_SOURCES += common/atomic_io.cc
replicant_daemon_SOURCES += common/bootstrap.cc
replicant_daemon_SOURCES += common/configuration.cc
replicant_daemon_SOURCES += common/generate_token.cc
replicant_daemon_SOURCES += common/ids.cc
replicant_daemon_SOURCES += common/network_msgtype.cc
replicant_daemon_SOURCES += common/packing.cc
replicant_daemon_SOURCES += common/server.cc
replicant_daemon_SOURCES += daemon/acceptor.cc
replicant_daemon_SOURCES += daemon/ballot.cc
replicant_daemon_SOURCES += daemon/commander.cc
replicant_daemon_SOURCES += daemon/condition.cc
replicant_daemon_SOURCES += daemon/daemon.cc
replicant_daemon_SOURCES += daemon/failure_tracker.cc
replicant_daemon_SOURCES += daemon/leader.cc
replicant_daemon_SOURCES += daemon/main.cc
replicant_daemon_SOURCES += daemon/mapper.cc
replicant_daemon_SOURCES += daemon/object.cc
replicant_daemon_SOURCES += daemon/pvalue.cc
replicant_daemon_SOURCES += daemon/replica.cc
replicant_daemon_SOURCES += daemon/robust_history.cc
replicant_daemon_SOURCES += daemon/scout.cc
replicant_daemon_SOURCES += daemon/settings.cc
replicant_daemon_SOURCES += daemon/slot_type.cc
replicant_daemon_SOURCES += daemon/snapshot.cc
replicant_daemon_SOURCES += daemon/unordered_command.cc
replicant_daemon_LDADD =
replicant_daemon_LDADD += $(BUSYBEE_LIBS)
replicant_daemon_LDADD += $(E_LIBS)
replicant_daemon_LDADD += $(POPT_LIBS)
replicant_daemon_LDADD += $(GLOG_LIBS)
replicant_daemon_LDADD += $(RT_LIBS)
replicant_daemon_LDADD += -lpthread

################################################################################
################################# State Machine ################################
################################################################################

lib_LTLIBRARIES += librsm.la
noinst_LTLIBRARIES += libreplicant_object_interface.la
include_HEADERS += include/rsm.h

replicantexec_PROGRAMS += replicant-rsm-dlopen

librsm_la_SOURCES = daemon/rsm.cc
librsm_la_LIBADD = libreplicant_object_interface.la $(E_LIBS)

libreplicant_object_interface_la_SOURCES = daemon/object_interface.cc

replicant_rsm_dlopen_SOURCES = daemon/rsm-dlopen.c
replicant_rsm_dlopen_LDADD =
replicant_rsm_dlopen_LDADD += librsm.la
replicant_rsm_dlopen_LDADD += $(PO6_LIBS)
replicant_rsm_dlopen_LDADD += $(LDL_LIBS)

################################################################################
#################################### Client ####################################
################################################################################

lib_LTLIBRARIES += libreplicant.la
include_HEADERS += include/replicant.h
pkgconfig_DATA += replicant.pc

noinst_HEADERS += client/client.h
noinst_HEADERS += client/mapper.h
noinst_HEADERS += client/pending_call.h
noinst_HEADERS += client/pending_call_robust.h
noinst_HEADERS += client/pending_cond_wait.h
noinst_HEADERS += client/pending_generate_unique_number.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_defended_call.h
noinst_HEADERS += client/pending_cond_follow.h
noinst_HEADERS += client/pending_robust.h
noinst_HEADERS += client/pending_poke.h
noinst_HEADERS += client/server_selector.h
noinst_HEADERS += client/server_status.h

libreplicant_la_SOURCES =
libreplicant_la_SOURCES += common/atomic_io.cc
libreplicant_la_SOURCES += common/bootstrap.cc
libreplicant_la_SOURCES += common/configuration.cc
libreplicant_la_SOURCES += common/generate_token.cc
libreplicant_la_SOURCES += common/ids.cc
libreplicant_la_SOURCES += common/network_msgtype.cc
libreplicant_la_SOURCES += common/packing.cc
libreplicant_la_SOURCES += common/server.cc
libreplicant_la_SOURCES += client/c.cc
libreplicant_la_SOURCES += client/client.cc
libreplicant_la_SOURCES += client/mapper.cc
libreplicant_la_SOURCES += client/pending_call.cc
libreplicant_la_SOURCES += client/pending_call_robust.cc
libreplicant_la_SOURCES += client/pending.cc
libreplicant_la_SOURCES += client/pending_cond_follow.cc
libreplicant_la_SOURCES += client/pending_cond_wait.cc
libreplicant_la_SOURCES += client/pending_defended_call.cc
libreplicant_la_SOURCES += client/pending_generate_unique_number.cc
libreplicant_la_SOURCES += client/pending_poke.cc
libreplicant_la_SOURCES += client/pending_robust.cc
libreplicant_la_SOURCES += client/server_selector.cc
libreplicant_la_SOURCES += client/server_status.cc
libreplicant_la_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
libreplicant_la_LIBADD =
libreplicant_la_LIBADD += $(BUSYBEE_LIBS)
libreplicant_la_LIBADD += $(E_LIBS)
libreplicant_la_LIBADD += $(PO6_LIBS)
libreplicant_la_LDFLAGS = -version-info 2:2:0

################################################################################
##################################### Tools ####################################
################################################################################

bin_PROGRAMS += replicant
replicantexec_PROGRAMS += replicant-debug
replicantexec_PROGRAMS += replicant-poke
replicantexec_PROGRAMS += replicant-generate-unique-number
replicantexec_PROGRAMS += replicant-new-object
replicantexec_PROGRAMS += replicant-del-object
replicantexec_PROGRAMS += replicant-kill-object
replicantexec_PROGRAMS += replicant-backup-object
replicantexec_PROGRAMS += replicant-restore-object
replicantexec_PROGRAMS += replicant-list-objects
replicantexec_PROGRAMS += replicant-conn-str
replicantexec_PROGRAMS += replicant-kill-server
replicantexec_PROGRAMS += replicant-server-status
replicantexec_PROGRAMS += replicant-availability-check
replicantexec_PROGRAMS += replicant-debug-call
replicantexec_PROGRAMS += replicant-debug-condition
replicantexec_PROGRAMS += replicant-debug-defended-call

noinst_HEADERS += tools/common.h

replicant_SOURCES = replicant.cc
replicant_CPPFLAGS = $(AM_CPPFLAGS) $(CPPFLAGS)
replicant_LDADD =
replicant_LDADD += $(E_LIBS)
replicant_LDADD += $(PO6_LIBS)
replicant_LDADD += $(POPT_LIBS)

replicant_debug_SOURCES = replicant-debug.cc
replicant_debug_CPPFLAGS = $(AM_CPPFLAGS) $(CPPFLAGS)
replicant_debug_LDADD =
replicant_debug_LDADD += $(E_LIBS)
replicant_debug_LDADD += $(PO6_LIBS)
replicant_debug_LDADD += $(POPT_LIBS)

replicant_poke_SOURCES = tools/poke.cc
replicant_poke_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_generate_unique_number_SOURCES = tools/generate-unique-number.cc
replicant_generate_unique_number_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_new_object_SOURCES = tools/new-object.cc
replicant_new_object_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_del_object_SOURCES = tools/del-object.cc
replicant_del_object_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_kill_object_SOURCES = tools/kill-object.cc
replicant_kill_object_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_backup_object_SOURCES = tools/backup-object.cc common/atomic_io.cc
replicant_backup_object_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_restore_object_SOURCES = tools/restore-object.cc common/atomic_io.cc
replicant_restore_object_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_list_objects_SOURCES = tools/list-objects.cc
replicant_list_objects_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_conn_str_SOURCES = tools/conn-str.cc
replicant_conn_str_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_kill_server_SOURCES = tools/kill-server.cc
replicant_kill_server_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_server_status_SOURCES = tools/server-status.cc
replicant_server_status_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_availability_check_SOURCES = tools/availability-check.cc
replicant_availability_check_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

replicant_debug_call_SOURCES = tools/debug-call.cc
replicant_debug_call_LDADD = libreplicant.la $(E_LIBS) $(PO6_LIBS) $(POPT_LIBS)

replicant_debug_condition_SOURCES = tools/debug-condition.cc
replicant_debug_condition_LDADD = libreplicant.la $(E_LIBS) $(PO6_LIBS) $(POPT_LIBS)

replicant_debug_defended_call_SOURCES = tools/debug-defended-call.cc
replicant_debug_defended_call_LDADD = libreplicant.la $(PO6_LIBS) $(POPT_LIBS)

################################################################################
################################### Examples ###################################
################################################################################

if ENABLE_EXAMPLES
replicantexec_LTLIBRARIES += libreplicant-example-tick.la
replicantexec_LTLIBRARIES += libreplicant-example-condition.la
replicantexec_LTLIBRARIES += libreplicant-example-counter.la
replicantexec_LTLIBRARIES += libreplicant-example-echo.la
replicantexec_LTLIBRARIES += libreplicant-example-log.la
replicantexec_LTLIBRARIES += libreplicant-example-nop.la
TESTS += test/example-tick.gremlin
TESTS += test/example-tick.valgrind.gremlin
TESTS += test/example-condition.gremlin
TESTS += test/example-condition.valgrind.gremlin
TESTS += test/example-counter.gremlin
TESTS += test/example-counter.valgrind.gremlin
TESTS += test/example-echo.gremlin
TESTS += test/example-echo.valgrind.gremlin
TESTS += test/example-log.gremlin
TESTS += test/example-log.valgrind.gremlin
TESTS += test/example-nop.gremlin
TESTS += test/example-nop.valgrind.gremlin
endif

libreplicant_example_tick_la_SOURCES = examples/tick.c
libreplicant_example_tick_la_CFLAGS = $(CFLAGS)
libreplicant_example_tick_la_LIBADD = librsm.la
check_SCRIPTS += test/example-tick.gremlin
check_SCRIPTS += test/example-tick.valgrind.gremlin
EXTRA_DIST += test/example-tick.gremlin
EXTRA_DIST += test/example-tick.valgrind.gremlin

libreplicant_example_condition_la_SOURCES = examples/condition.c
libreplicant_example_condition_la_CFLAGS = $(CFLAGS)
libreplicant_example_condition_la_LIBADD = librsm.la
check_SCRIPTS += test/example-condition.gremlin
check_SCRIPTS += test/example-condition.valgrind.gremlin
EXTRA_DIST += test/example-condition.gremlin
EXTRA_DIST += test/example-condition.valgrind.gremlin

libreplicant_example_counter_la_SOURCES = examples/counter.c
libreplicant_example_counter_la_CFLAGS = $(CFLAGS)
libreplicant_example_counter_la_LIBADD = librsm.la
check_SCRIPTS += test/example-counter.gremlin
check_SCRIPTS += test/example-counter.valgrind.gremlin
EXTRA_DIST += test/example-counter.gremlin
EXTRA_DIST += test/example-counter.valgrind.gremlin

libreplicant_example_echo_la_SOURCES = examples/echo.c
libreplicant_example_echo_la_CFLAGS = $(CFLAGS)
libreplicant_example_echo_la_LIBADD = librsm.la
check_SCRIPTS += test/example-echo.gremlin
check_SCRIPTS += test/example-echo.valgrind.gremlin
EXTRA_DIST += test/example-echo.gremlin
EXTRA_DIST += test/example-echo.valgrind.gremlin

libreplicant_example_log_la_SOURCES = examples/log.c
libreplicant_example_log_la_CFLAGS = $(CFLAGS)
libreplicant_example_log_la_LIBADD = librsm.la
check_SCRIPTS += test/example-log.gremlin
check_SCRIPTS += test/example-log.valgrind.gremlin
EXTRA_DIST += test/example-log.gremlin
EXTRA_DIST += test/example-log.valgrind.gremlin

libreplicant_example_nop_la_SOURCES = examples/nop.c
libreplicant_example_nop_la_CFLAGS = $(CFLAGS)
libreplicant_example_nop_la_LIBADD = librsm.la
check_SCRIPTS += test/example-nop.gremlin
check_SCRIPTS += test/example-nop.valgrind.gremlin
EXTRA_DIST += test/example-nop.gremlin
EXTRA_DIST += test/example-nop.valgrind.gremlin

################################################################################
##################################### Tests ####################################
################################################################################

EXTRA_DIST += test/env.sh
EXTRA_DIST += replicant.supp

check_SCRIPTS += test/5-node-cluster.gremlin
check_SCRIPTS += test/5-node-cluster.valgrind.gremlin
check_SCRIPTS += test/chaos.gremlin
check_SCRIPTS += test/chaos.valgrind.gremlin
check_SCRIPTS += test/restart-diff-address.gremlin
check_SCRIPTS += test/restart-diff-address.valgrind.gremlin
check_SCRIPTS += test/leader-rotate.gremlin
check_SCRIPTS += test/leader-rotate.valgrind.gremlin
EXTRA_DIST += test/5-node-cluster.gremlin
EXTRA_DIST += test/5-node-cluster.valgrind.gremlin
EXTRA_DIST += test/chaos.gremlin
EXTRA_DIST += test/chaos.valgrind.gremlin
EXTRA_DIST += test/restart-diff-address.gremlin
EXTRA_DIST += test/restart-diff-address.valgrind.gremlin
EXTRA_DIST += test/leader-rotate.gremlin
EXTRA_DIST += test/leader-rotate.valgrind.gremlin

TESTS += test/5-node-cluster.gremlin
TESTS += test/5-node-cluster.valgrind.gremlin

if ENABLE_EXAMPLES
TESTS += test/chaos.gremlin
TESTS += test/chaos.valgrind.gremlin
TESTS += test/restart-diff-address.gremlin
TESTS += test/restart-diff-address.valgrind.gremlin
TESTS += test/leader-rotate.gremlin
TESTS += test/leader-rotate.valgrind.gremlin
endif

################################################################################
################################### Benchmark ##################################
################################################################################

EXTRA_PROGRAMS += replicant-benchmark

replicant_benchmark_SOURCES = replicant-benchmark.cc
replicant_benchmark_LDADD = libreplicant.la -lygor -lpthread $(POPT_LIBS)

################################################################################
################################## Maintenance #################################
################################################################################

EXTRA_DIST += maint/valgrind-gremlins

maintainer-generate:
	maint/valgrind-gremlins
