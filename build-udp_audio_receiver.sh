#!/bin/bash
g++ -O2 -march=native -std=c++17 udp_audio_receiver.cpp \
    -o udp_rx -lpthread -lpulse -lpulse-simple

# ./udp_rx                              # Standard-Sink, Port 5000
# ./udp_rx --sink alsa_output.pci-0000_00_1f.3.analog-stereo
# ./udp_rx --gain 1.5 --prebuffer-ms 100
# pactl list short sinks                # verfuegbare Sinks auflisten
