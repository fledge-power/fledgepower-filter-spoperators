#include "version.h"

#include <plugin_api.h>

#include <rapidjson/document.h>
#include <gtest/gtest.h>


using namespace rapidjson;

extern "C" {
    PLUGIN_INFORMATION* plugin_info();
};

TEST(PluginInfoTest, PluginInfo)
{
    PLUGIN_INFORMATION* info = nullptr;
    ASSERT_NO_THROW(info = plugin_info());
    ASSERT_NE(info, nullptr);
    ASSERT_STREQ(info->name, "spoperators");
    ASSERT_STREQ(info->version, VERSION);
    ASSERT_EQ(info->options, 0);
    ASSERT_EQ(info->type, PLUGIN_TYPE_FILTER);
    ASSERT_STREQ(info->interface, "1.0.0");
}

TEST(PluginInfoTest, PluginInfoConfigParse)
{
    PLUGIN_INFORMATION* info = nullptr;
    ASSERT_NO_THROW(info = plugin_info());
    ASSERT_NE(info, nullptr);
    Document doc;
    doc.Parse(info->config);
    ASSERT_EQ(doc.HasParseError(), false);
    ASSERT_EQ(doc.IsObject(), true);
    ASSERT_EQ(doc.HasMember("plugin"), true);
    ASSERT_EQ(doc.HasMember("enable"), true);
    ASSERT_EQ(doc.HasMember("exchanged_data"), true);
}
