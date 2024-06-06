#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filterOperationSp.h>

using namespace std;

extern "C" {
	PLUGIN_INFORMATION *plugin_info();
	PLUGIN_HANDLE plugin_init(ConfigCategory *config,
			  OUTPUT_HANDLE *outHandle,
			  OUTPUT_STREAM output);

    void plugin_shutdown(PLUGIN_HANDLE handle);
};

TEST(PluginInitShutdownTest, PluginInitNoConfig)
{
    PLUGIN_HANDLE handle = nullptr;
	ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
    ASSERT_TRUE(handle != nullptr);

	FilterOperationSp* filter = static_cast<FilterOperationSp*>(handle);
	ASSERT_EQ(filter->isEnabled(), true);

    ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE>(handle)));
}

TEST(PluginInitShutdownTest, PluginShutdown) 
{
	PLUGIN_INFORMATION *info = nullptr;
    ASSERT_NO_THROW(info = plugin_info());
    ASSERT_NE(info, nullptr);

	ConfigCategory *config = nullptr;
    ASSERT_NO_THROW(config = new ConfigCategory("operationsp", info->config));
	ASSERT_NE(config, nullptr);		
	ASSERT_NO_THROW(config->setItemsValueFromDefault());
	
	PLUGIN_HANDLE handle = nullptr;
	ASSERT_NO_THROW(handle = plugin_init(config, nullptr, nullptr));
	ASSERT_TRUE(handle != nullptr);

	FilterOperationSp* filter = static_cast<FilterOperationSp*>(handle);
	ASSERT_EQ(filter->isEnabled(), true);

	ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE>(handle)));
}