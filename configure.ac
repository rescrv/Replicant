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
#
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([replicant], [0.2.dev], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-bzip2])
AM_SILENT_RULES([yes])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([client/replicant.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG(C++)

ANAL_WARNINGS

# Checks for libraries.
AC_CHECK_LIB([popt], [poptGetContext], [], [AC_MSG_ERROR([
---------------------------------------
Replicant relies upon the popt library.
Please install popt to continue.
---------------------------------------])])
AC_CHECK_LIB([rt], [clock_gettime], [needs_lrt=yes], [needs_lrt=no])

if test x"${needs_lrt}" = xyes; then
    AC_SUBST([LRT_LIBS], ["-lrt"])
else
    AC_SUBST([LRT_LIBS], [""])
fi

PKG_CHECK_MODULES([PO6], [libpo6 >= 0.3.1])
PKG_CHECK_MODULES([E], [libe >= 0.3.2])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.3.0])
PKG_CHECK_MODULES([HYPERLEVELDB], [libhyperleveldb >= 2.0.dev])

# Checks for header files.
AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
---------------------------------------
Replicant relies upon the popt library.
Please install popt to continue.
---------------------------------------])])
AC_CHECK_HEADER([glog/logging.h],,[AC_MSG_ERROR([
---------------------------------------
Replicant relies upon the glog library.
Please install glog to continue.
---------------------------------------])])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_ARG_ENABLE([log-commands], [AS_HELP_STRING([--enable-log-commands],
              [enable code to log all commands @<:@default: no@:>@])],
              [enable_log_commands=${enableval}], [enable_log_commands=no])
if test x"${enable_log_commands}" = xyes; then
    AC_DEFINE([REPL_LOG_COMMANDS], [], [Log all commands that are submitted, issued and acknowledged])
fi

AC_ARG_ENABLE([log-suspicions], [AS_HELP_STRING([--enable-log-suspicions],
              [enable code to log suspicions @<:@default: no@:>@])],
              [enable_log_suspicions=${enableval}], [enable_log_suspicions=no])
if test x"${enable_log_suspicions}" = xyes; then
    AC_DEFINE([REPL_LOG_SUSPICIONS], [], [Log the suspicion level of other nodes])
fi

AC_ARG_ENABLE([example_state_machines], [AS_HELP_STRING([--enable-example-state-machines],
              [build Python bindings @<:@default: no@:>@])],
              [example_state_machines=${enableval}], [example_state_machines=no])
AM_CONDITIONAL([ENABLE_EXAMPLES], [test x"${example_state_machines}" = xyes])

AC_CHECK_FUNCS([clock_gettime mach_absolute_time])

AC_CONFIG_FILES([Makefile
                 replicant.pc])
AC_OUTPUT
