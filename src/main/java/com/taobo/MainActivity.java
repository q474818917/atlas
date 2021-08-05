package com.taobo;

import io.appium.java_client.AppiumDriver;
import io.appium.java_client.MobileElement;
import io.appium.java_client.remote.AndroidMobileCapabilityType;
import io.appium.java_client.remote.MobileCapabilityType;
import org.apdplat.word.WordFrequencyStatistics;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
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
            //根据ID获取页面元素
           MobileElement element =  driver.findElementById("");
           //页面刷新
           driver.navigate().refresh();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        /*WordFrequencyStatistics wordFrequencyStatistics = new WordFrequencyStatistics();
        wordFrequencyStatistics.setRemoveStopWord(false);
        wordFrequencyStatistics.setResultPath("word-frequency-statistics.txt");
        wordFrequencyStatistics.setSegmentationAlgorithm(SegmentationAlgorithm.MaxNgramScore);
        //开始分词
        wordFrequencyStatistics.seg("阿根廷");
        wordFrequencyStatistics.dump();*/
    }
}
