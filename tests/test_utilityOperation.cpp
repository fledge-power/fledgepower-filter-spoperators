#include <gtest/gtest.h>

#include "utilityOperation.h"

TEST(OperationUtilityTest, Join)
{
    ASSERT_STREQ(UtilityOperation::join({}).c_str(), "");
    ASSERT_STREQ(UtilityOperation::join({"TEST"}).c_str(), "TEST");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "TOAST", "TASTE"}).c_str(), "TEST, TOAST, TASTE");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "", "TORTOISE"}).c_str(), "TEST, , TORTOISE");
    ASSERT_STREQ(UtilityOperation::join({}, "-").c_str(), "");
    ASSERT_STREQ(UtilityOperation::join({"TEST"}, "-").c_str(), "TEST");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "TOAST", "TASTE"}, "-").c_str(), "TEST-TOAST-TASTE");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "", "TORTOISE"}, "-").c_str(), "TEST--TORTOISE");
    ASSERT_STREQ(UtilityOperation::join({}, "").c_str(), "");
    ASSERT_STREQ(UtilityOperation::join({"TEST"}, "").c_str(), "TEST");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "TOAST", "TASTE"}, "").c_str(), "TESTTOASTTASTE");
    ASSERT_STREQ(UtilityOperation::join({"TEST", "", "TORTOISE"}, "").c_str(), "TESTTORTOISE");
}

TEST(OperationUtilityTest, Split)
{
    ASSERT_EQ(UtilityOperation::split("", '-'), std::vector<std::string>{""});
    ASSERT_EQ(UtilityOperation::split("TEST", '-'), std::vector<std::string>{"TEST"});
    std::vector<std::string> out1{"TEST", "TOAST", "TASTE"};
    ASSERT_EQ(UtilityOperation::split("TEST-TOAST-TASTE", '-'), out1);
    std::vector<std::string> out2{"TEST", "", "TORTOISE"};
    ASSERT_EQ(UtilityOperation::split("TEST--TORTOISE", '-'), out2);
}

TEST(OperationUtilityTest, Logs)
{
    std::string text("This message is at level %s");
    ASSERT_NO_THROW(UtilityOperation::log_debug(text.c_str(), "debug"));
    ASSERT_NO_THROW(UtilityOperation::log_info(text.c_str(), "info"));
    ASSERT_NO_THROW(UtilityOperation::log_warn(text.c_str(), "warning"));
    ASSERT_NO_THROW(UtilityOperation::log_error(text.c_str(), "error"));
    ASSERT_NO_THROW(UtilityOperation::log_fatal(text.c_str(), "fatal"));
}