## Copyright (c) 2012-2013, Robert Escriva
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

AM_CPPFLAGS  = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS)
AM_CFLAGS    = $(WANAL_CFLAGS)
AM_CXXFLAGS  = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory

pkgconfigdir = $(libdir)/pkgconfig
replicantexecdir = $(libexec)/$(PACKAGE)-$(VERSION)

EXTRA_DIST =

include_HEADERS =
noinst_HEADERS =

lib_LTLIBRARIES =

bin_PROGRAMS =
noinst_PROGRAMS =

replicantexec_PROGRAMS =
replicantexec_LTLIBRARIES =

pkgconfig_DATA =

################################################################################
#################################### Common ####################################
################################################################################

EXTRA_DIST += LICENSE

noinst_HEADERS += common/bootstrap.h
noinst_HEADERS += common/chain_node.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/mapper.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/packing.h
noinst_HEADERS += common/response_returncode.h
noinst_HEADERS += common/special_objects.h

################################################################################
#################################### Daemon ####################################
################################################################################

replicantexec_PROGRAMS += replicant-daemon
lib_LTLIBRARIES += libreplicant-state-machine.la
include_HEADERS += daemon/replicant_state_machine.h

noinst_HEADERS += daemon/conditions_wrapper.h
noinst_HEADERS += daemon/configuration_manager.h
noinst_HEADERS += daemon/connection.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/fact_store.h
noinst_HEADERS += daemon/failure_detector.h
noinst_HEADERS += daemon/heal_next.h
noinst_HEADERS += daemon/memstream.h
noinst_HEADERS += daemon/object_manager.h
noinst_HEADERS += daemon/replicant_state_machine_context.h
noinst_HEADERS += daemon/replicant_state_machine.h
noinst_HEADERS += daemon/request_response.h
noinst_HEADERS += daemon/settings.h

replicant_daemon_SOURCES =
replicant_daemon_SOURCES += common/bootstrap.cc
replicant_daemon_SOURCES += common/chain_node.cc
replicant_daemon_SOURCES += common/configuration.cc
replicant_daemon_SOURCES += common/mapper.cc
replicant_daemon_SOURCES += common/network_msgtype.cc
replicant_daemon_SOURCES += common/packing.cc
replicant_daemon_SOURCES += common/response_returncode.cc
replicant_daemon_SOURCES += daemon/configuration_manager.cc
replicant_daemon_SOURCES += daemon/connection.cc
replicant_daemon_SOURCES += daemon/daemon.cc
replicant_daemon_SOURCES += daemon/fact_store.cc
replicant_daemon_SOURCES += daemon/failure_detector.cc
replicant_daemon_SOURCES += daemon/heal_next.cc
replicant_daemon_SOURCES += daemon/main.cc
replicant_daemon_SOURCES += daemon/object_manager.cc
replicant_daemon_SOURCES += daemon/settings.cc
replicant_daemon_LDADD =
replicant_daemon_LDADD += $(E_LIBS)
replicant_daemon_LDADD += $(BUSYBEE_LIBS)
replicant_daemon_LDADD += $(HYPERLEVELDB_LIBS)
replicant_daemon_LDADD += libreplicant-state-machine.la
replicant_daemon_LDADD += -lglog -ldl -lpthread

libreplicant_state_machine_la_SOURCES = daemon/replicant_state_machine_context.cc

################################################################################
#################################### Client ####################################
################################################################################

lib_LTLIBRARIES += libreplicant.la
include_HEADERS += client/replicant.h
pkgconfig_DATA += replicant.pc

noinst_HEADERS += client/command.h
noinst_HEADERS += client/replicant.h

libreplicant_la_SOURCES =
libreplicant_la_SOURCES += common/bootstrap.cc
libreplicant_la_SOURCES += common/chain_node.cc
libreplicant_la_SOURCES += common/configuration.cc
libreplicant_la_SOURCES += common/mapper.cc
libreplicant_la_SOURCES += common/network_msgtype.cc
libreplicant_la_SOURCES += common/packing.cc
libreplicant_la_SOURCES += common/response_returncode.cc
libreplicant_la_SOURCES += client/command.cc
libreplicant_la_SOURCES += client/replicant.cc
libreplicant_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)
libreplicant_la_LIBADD = $(E_LIBS) $(BUSYBEE_LIBS)

################################################################################
##################################### Tools ####################################
################################################################################

bin_PROGRAMS += replicant
noinst_PROGRAMS += replicant-block
noinst_PROGRAMS += replicant-cli
replicantexec_PROGRAMS += replicant-new-object
replicantexec_PROGRAMS += replicant-del-object
replicantexec_PROGRAMS += replicant-repair

noinst_HEADERS += tools/common.h

replicant_SOURCES = replicant.cc
replicant_CPPFLAGS = -DREPLICANT_EXEC_DIR="\"$(replicantexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)

replicant_block_SOURCES = tools/block.cc
replicant_block_LDADD = libreplicant.la

replicant_cli_SOURCES = tools/cli.cc
replicant_cli_LDADD = libreplicant.la

replicant_new_object_SOURCES = tools/new-object.cc
replicant_new_object_LDADD = libreplicant.la

replicant_del_object_SOURCES = tools/del-object.cc
replicant_del_object_LDADD = libreplicant.la

replicant_repair_SOURCES = tools/repair.cc common/chain_node.cc common/configuration.cc common/packing.cc daemon/configuration_manager.cc daemon/fact_store.cc
replicant_repair_LDADD = $(E_LIBS) $(HYPERLEVELDB_LIBS) -lglog

################################################################################
################################### Examples ###################################
################################################################################

if ENABLE_EXAMPLES
replicantexec_LTLIBRARIES += libreplicant-example-condition.la
replicantexec_LTLIBRARIES += libreplicant-example-echo.la
replicantexec_LTLIBRARIES += libreplicant-example-log.la
replicantexec_LTLIBRARIES += libreplicant-example-nop.la
endif

libreplicant_example_condition_la_SOURCES = examples/condition.c
libreplicant_example_condition_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(AM_CPPFLAGS) $(CPPFLAGS)

libreplicant_example_echo_la_SOURCES = examples/echo.c
libreplicant_example_echo_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(AM_CPPFLAGS) $(CPPFLAGS)

libreplicant_example_log_la_SOURCES = examples/log.c
libreplicant_example_log_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(AM_CPPFLAGS) $(CPPFLAGS)

libreplicant_example_nop_la_SOURCES = examples/nop.c
libreplicant_example_nop_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(AM_CPPFLAGS) $(CPPFLAGS)
