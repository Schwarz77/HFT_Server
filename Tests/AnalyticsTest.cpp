// AnalyticsTest.cpp

#include <gtest/gtest.h>
#include "Analytics.h"

TEST(AnalyticsTest, RollingVWAPAccuracy) {
    const int WINDOW_SIZE = 3;
    RollingVWAP<WINDOW_SIZE> vwap;

    // 1st event (100.0, 2.0); // 100 * 2 / 2 = 100
    vwap.add(100.0, 2.0); // 100 * 2 / 2 = 100
    EXPECT_NEAR(vwap.value(), 100.0, 0.0001);

    // 2nd event
    vwap.add(200.0, 2.0); // (200 + 400) / 4 = 150
    EXPECT_NEAR(vwap.value(), 150.0, 0.0001);

    // 3rd event (fill window)
    vwap.add(300.0, 6.0); // (200 + 400 + 1800) / 10 = 240
    EXPECT_NEAR(vwap.value(), 240.0, 0.0001);

    // 4th event (1st must must leave the window)
    // Leaves (100, 2.0), adds (400, 2.0)
    // New state: (200, 2.0), (300, 6.0), (400, 2.0)
    // PV sum: 400 + 1800 + 800 = 3000
    // V sum: 2 + 6 + 2 = 10
    vwap.add(400.0, 2.0); 
    EXPECT_NEAR(vwap.value(), 300.0, 0.0001);
}

