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

AM_CXXFLAGS = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS)
AM_LDFLAGS = $(PO6_LDFLAGS) $(E_LDFLAGS) $(BUSYBEE_LDFLAGS)

lib_LTLIBRARIES = libreplicant.la

include_HEADERS = \
			client/replicant.h \
			daemon/replicant_state_machine.h

noinst_HEADERS = \
			common/chain_node.h \
			common/configuration.h \
			common/macros.h \
			common/network_msgtype.h \
			common/packing.h \
			common/special_objects.h \
			client/command.h \
			daemon/client_manager.h \
			daemon/command.h \
			daemon/command_manager.h \
			daemon/configuration_manager.h \
			daemon/daemon.h \
			daemon/heal_next.h \
			daemon/object_manager.h \
			daemon/settings.h \
			tools/common.h

bin_PROGRAMS = replicant

replicantexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
replicantexec_PROGRAMS = \
			replicant-cli \
			replicant-create-object \
			replicant-daemon 

replicant_SOURCES = replicant.cc
			
replicant_CPPFLAGS = \
			-DREPLICANT_EXEC_DIR="\"$(replicantexecdir)\""

replicant_daemon_SOURCES = \
			common/chain_node.cc \
			common/configuration.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			daemon/client_manager.cc \
			daemon/command.cc \
			daemon/command_manager.cc \
			daemon/configuration_manager.cc \
			daemon/daemon.cc \
			daemon/heal_next.cc \
			daemon/main.cc \
			daemon/object_manager.cc \
			daemon/settings.cc
replicant_daemon_LDADD = -le -lbusybee-sta -lglog -lrt -ldl -lpthread

libreplicant_la_CXXFLAGS = \
			$(CXXFLAGS) \
			$(AM_CXXFLAGS)

libreplicant_la_LDFLAGS = \
			$(AM_LDFLAGS)

libreplicant_la_SOURCES = \
			common/chain_node.cc \
			common/configuration.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			client/command.cc \
			client/replicant.cc
libreplicant_la_LIBADD = -le -lbusybee-st -lrt

replicant_cli_SOURCES = \
			tools/cli.cc
replicant_cli_LDADD = libreplicant.la

replicant_create_object_SOURCES = \
			tools/create-object.cc
replicant_create_object_LDADD = libreplicant.la

