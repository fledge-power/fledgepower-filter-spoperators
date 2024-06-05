#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filterOperationSp.h>

using namespace std;

static string reconfigure = QUOTE({
    "enable": {
        "value": "false"
    }
});

extern "C" {
	PLUGIN_HANDLE plugin_init(ConfigCategory *config,
			  OUTPUT_HANDLE *outHandle,
			  OUTPUT_STREAM output);

    void plugin_reconfigure(PLUGIN_HANDLE handle, const string& newConfig);
    void plugin_shutdown(PLUGIN_HANDLE handle);
};

class PluginReconfigure : public testing::Test
{
protected:
    FilterOperationSp *filter = nullptr;  // Object on which we call for tests

    // Setup is ran for every tests, so each variable are reinitialised
    void SetUp() override
    {
		void *handle = nullptr;
        ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
		filter = (FilterOperationSp *) handle;
        ASSERT_NE(filter, nullptr);
    }

    // TearDown is ran for every tests, so each variable are destroyed again
    void TearDown() override
    {
        if (filter) {
            ASSERT_NO_THROW(plugin_shutdown(reinterpret_cast<PLUGIN_HANDLE*>(filter)));
        }
    }   
};

TEST_F(PluginReconfigure, Reconfigure) 
{
	plugin_reconfigure((PLUGIN_HANDLE*)filter, reconfigure);
    ASSERT_EQ(filter->isEnabled(), false);
}