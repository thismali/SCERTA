//
//  ViewController.m
//  iosDemo5543Lab1
//
//  Created by Ali, Mohamoud (UMKC-Student) on 9/13/15.
//  Copyright (c) 2015 Ali, Mohamoud (UMKC-Student). All rights reserved.
//

#import "ViewController.h"
#import "ViewControllerSecond.h"

@interface ViewController ()


@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view, typically from a nib.
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

- (IBAction)handleTouchUp:(id)sender {
    
    ViewControllerSecond *thisController = [self.storyboard instantiateViewControllerWithIdentifier:@"ViewControllerSecond"];
    
    [self presentViewController:thisController animated:NO completion:NULL];
    
}


@end
