#
# Find PipeWire (libpipewire-0.3) via pkg-config, fallback to manual search
#
if(PIPEWIRE_INCLUDE_DIRS AND PIPEWIRE_LIBRARIES)
  set(PIPEWIRE_FIND_QUIETLY true)
endif()

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(PIPEWIRE libpipewire-0.3)
endif()

if(NOT PIPEWIRE_FOUND)
  find_path(PIPEWIRE_INCLUDE_DIRS pipewire/pipewire.h
    PATH_SUFFIXES pipewire-0.3)
  find_library(PIPEWIRE_LIBRARIES pipewire-0.3)
  if(PIPEWIRE_INCLUDE_DIRS AND PIPEWIRE_LIBRARIES)
    set(PIPEWIRE_FOUND true)
  endif()
endif()

if(PIPEWIRE_FOUND)
  if(NOT PIPEWIRE_FIND_QUIETLY)
    message(STATUS "Found PipeWire: ${PIPEWIRE_LIBRARIES}")
  endif()
else()
  if(PIPEWIRE_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find PipeWire (libpipewire-0.3)")
  endif()
  if(NOT PIPEWIRE_FIND_QUIETLY)
    message(STATUS "Could not find PipeWire")
  endif()
endif()

mark_as_advanced(PIPEWIRE_INCLUDE_DIRS PIPEWIRE_LIBRARIES)
