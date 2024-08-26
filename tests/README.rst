*****************************************************
Unit Test for filter plugin make operations with multiple status points to produce a value for a single status point
*****************************************************

Require Google Unit Test framework

Install with:
::
    sudo apt-get install libgtest-dev
    cd /usr/src/gtest
    cmake CMakeLists.txt
    sudo make
    sudo make install

To build the unit tests:
::
    mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make
    ./RunTests