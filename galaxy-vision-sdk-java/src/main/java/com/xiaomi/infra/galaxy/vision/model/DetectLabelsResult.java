package com.xiaomi.infra.galaxy.vision.model;

import java.util.ArrayList;
import java.util.List;

public class DetectLabelsResult {
  private List<Label> labels;

  public List<Label> getLabels() {
    return labels;
  }

  public void setLabels(List<Label> labels) {
    this.labels = labels;
  }
  
  public void addLabel(Label... labels) {
    if (this.labels == null) {
      this.labels = new ArrayList<Label>();
    }
    
    for (Label label : labels) {
      this.labels.add(label);
    }
  }  
}
