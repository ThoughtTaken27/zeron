import XCTest
@testable import Zeron

final class HarnessCatalogTests: XCTestCase {
    func testCodexFallbackStartsWithAstraAndExposesItsTraits() {
        let models = HarnessCatalog.models(for: "codex")
        let astra = models.first

        XCTAssertEqual(astra?.id, "gpt-6-astra")
        XCTAssertEqual(astra?.label, "GPT-6-Astra")
        XCTAssertEqual(astra?.reasoningLevels,
                       ["low", "medium", "high", "xhigh", "max", "ultra"])
        XCTAssertEqual(astra?.options.first?.id, "serviceTier")
        XCTAssertEqual(astra?.options.first?.choices.map(\.id), ["default", "fast"])
    }

    func testAsideFallbackMirrorsRustRoutingCatalog() {
        XCTAssertEqual(HarnessCatalog.label(for: "aside"), "Aside")
        XCTAssertEqual(HarnessCatalog.models(for: "aside").map(\.id), ["default", "fast"])

        for model in HarnessCatalog.models(for: "aside") {
            // No reasoning ladder: the `effort` option is the single thinking
            // knob, so the Reasoning row stays hidden (mirrors the Rust rows).
            XCTAssertTrue(model.reasoningLevels.isEmpty, "\(model.id)")
            XCTAssertEqual(model.options.map(\.id), ["effort", "permission"])
        }

        let effort = HarnessCatalog.models(for: "aside")[0].options[0]
        XCTAssertEqual(effort.label, "Thinking Effort")
        XCTAssertEqual(effort.choices.map(\.id),
                       ["default", "off", "minimal", "low", "medium", "high",
                        "xhigh", "max", "ultrabrowse"])
        XCTAssertEqual(effort.defaultChoice, "default")

        let permission = HarnessCatalog.models(for: "aside")[0].options[1]
        XCTAssertEqual(permission.choices.map(\.id), ["ask", "guard", "full-access"])
        XCTAssertEqual(permission.defaultChoice, "guard")
    }

    func testAsideUsesMonochromeBrandAndOffLabel() {
        XCTAssertNotEqual(BrandMark.forHarness("aside").pathData,
                          BrandMark.forHarness("claude-code").pathData)
        XCTAssertNil(BrandMark.brandTint(for: "aside"))
        XCTAssertEqual(HarnessCatalog.reasoningLabel("off"), "Off")
    }

    func testDefaultReasoningMatchesDesktopPreference() {
        let astra = HarnessCatalog.defaultModel(for: "codex")
        XCTAssertEqual(HarnessCatalog.defaultReasoning(for: astra), "high")

        let short = ModelInfo(id: "short", label: "Short", description: nil,
                              reasoningLevels: ["low", "medium"])
        XCTAssertEqual(HarnessCatalog.defaultReasoning(for: short), "medium")
    }

    func testChoiceFallsBackToAdvertisedDefault() {
        let option = HarnessCatalog.defaultModel(for: "codex").options[0]
        XCTAssertEqual(HarnessCatalog.selectedChoice(for: option, selectedId: "fast").label, "Fast")
        XCTAssertEqual(HarnessCatalog.selectedChoice(for: option, selectedId: "stale").id, "default")
    }
}
