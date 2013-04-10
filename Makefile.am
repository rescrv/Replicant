# Copyright (c) 2012, Robert Escriva
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Replicant nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CXXFLAGS = $(WANAL_CXXFLAGS) $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(LRT_CFLAGS)
AM_LDFLAGS = $(PO6_LDFLAGS) $(E_LDFLAGS) $(BUSYBEE_LDFLAGS)

lib_LTLIBRARIES = libreplicant.la libreplicant-state-machine.la

include_HEADERS = \
			client/replicant.h \
			daemon/replicant_state_machine.h

noinst_HEADERS = \
			common/bootstrap.h \
			common/chain_node.h \
			common/configuration.h \
			common/macros.h \
			common/mapper.h \
			common/network_msgtype.h \
			common/packing.h \
			common/response_returncode.h \
			common/special_objects.h \
			client/command.h \
			daemon/conditions_wrapper.h \
			daemon/configuration_manager.h \
			daemon/connection.h \
			daemon/daemon.h \
			daemon/fact_store.h \
			daemon/failure_detector.h \
			daemon/heal_next.h \
			daemon/memstream.h \
			daemon/object_manager.h \
			daemon/settings.h \
			daemon/replicant_state_machine.h \
			daemon/replicant_state_machine_context.h \
			daemon/request_response.h \
			tools/common.h

bin_PROGRAMS = replicant

noinst_PROGRAMS = \
			replicant-block \
			replicant-cli

replicantexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
replicantexec_PROGRAMS = \
			replicant-daemon \
			replicant-repair \
			replicant-new-object \
			replicant-del-object

replicant_SOURCES = replicant.cc
replicant_CPPFLAGS = -DREPLICANT_EXEC_DIR="\"$(replicantexecdir)\""

libreplicant_state_machine_la_SOURCES = daemon/replicant_state_machine_context.cc
replicant_daemon_SOURCES = \
			common/bootstrap.cc \
			common/chain_node.cc \
			common/configuration.cc \
			common/mapper.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			common/response_returncode.cc \
			daemon/configuration_manager.cc \
			daemon/connection.cc \
			daemon/daemon.cc \
			daemon/fact_store.cc \
			daemon/failure_detector.cc \
			daemon/heal_next.cc \
			daemon/main.cc \
			daemon/object_manager.cc \
			daemon/settings.cc
replicant_daemon_LDADD = $(E_LIBS) $(BUSYBEE_LIBS) -lglog -lleveldb -ldl -lpthread libreplicant-state-machine.la

libreplicant_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)
libreplicant_la_SOURCES = \
			common/bootstrap.cc \
			common/chain_node.cc \
			common/configuration.cc \
			common/mapper.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			common/response_returncode.cc \
			client/command.cc \
			client/replicant.cc
libreplicant_la_LIBADD = $(E_LIBS) $(BUSYBEE_LIBS)

replicant_block_SOURCES = tools/block.cc
replicant_block_LDADD = libreplicant.la

replicant_repair_SOURCES = tools/repair.cc common/chain_node.cc common/configuration.cc common/packing.cc daemon/configuration_manager.cc daemon/fact_store.cc
replicant_repair_LDADD = $(E_LIBS) -lglog -lleveldb

replicant_cli_SOURCES = tools/cli.cc
replicant_cli_LDADD = libreplicant.la

replicant_new_object_SOURCES = tools/new-object.cc
replicant_new_object_LDADD = libreplicant.la

replicant_del_object_SOURCES = tools/del-object.cc
replicant_del_object_LDADD = libreplicant.la

if ENABLE_EXAMPLES
replicantexec_LTLIBRARIES = libreplicant-example-condition.la libreplicant-example-echo.la libreplicant-example-log.la libreplicant-example-nop.la
endif
libreplicant_example_condition_la_SOURCES = examples/condition.c
libreplicant_example_condition_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(CPPFLAGS)
libreplicant_example_echo_la_SOURCES = examples/echo.c
libreplicant_example_echo_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(CPPFLAGS)
libreplicant_example_log_la_SOURCES = examples/log.c
libreplicant_example_log_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(CPPFLAGS)
libreplicant_example_nop_la_SOURCES = examples/nop.c
libreplicant_example_nop_la_CPPFLAGS = -I$(abs_top_srcdir)/daemon/ $(CPPFLAGS)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = replicant.pc
