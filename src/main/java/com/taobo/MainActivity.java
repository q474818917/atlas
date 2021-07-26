package com.taobo;

import io.appium.java_client.AppiumDriver;
import io.appium.java_client.MobileElement;
import io.appium.java_client.remote.AndroidMobileCapabilityType;
import io.appium.java_client.remote.MobileCapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;

import java.net.MalformedURLException;
import java.net.URL;

public class MainActivity {

    public static void main(String[] args) {
        DesiredCapabilities capabilities = new DesiredCapabilities();
        capabilities.setCapability(MobileCapabilityType.PLATFORM_NAME, "");
        capabilities.setCapability(MobileCapabilityType.PLATFORM_VERSION, "");
        capabilities.setCapability(MobileCapabilityType.DEVICE_NAME, "");
        capabilities.setCapability(MobileCapabilityType.APP, "");

        capabilities.setCapability(MobileCapabilityType.NO_RESET, true);

        capabilities.setCapability(AndroidMobileCapabilityType.APP_PACKAGE, "");
        capabilities.setCapability(AndroidMobileCapabilityType.APP_ACTIVITY, "");

        try {
            AppiumDriver<MobileElement> driver = new AppiumDriver<>(
                    new URL("http://127.0.0.1:4723/wd/hub"),
                    capabilities);
           MobileElement element =  driver.findElementById("");

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }
}
